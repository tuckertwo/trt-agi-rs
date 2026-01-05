use asterisk_manager::{Manager, ManagerOptions, AmiAction, AmiResponse, AmiError, AmiEvent, HangupEventData};
use tokio::sync::{mpsc, oneshot, broadcast};
use tokio::select;
use tokio::time::{sleep, Duration};
use tokio_stream::StreamExt;
use anyhow::{anyhow, bail, Result};
use std::collections::HashMap;
use urlencoding::decode;
use log::{debug, error, log_enabled, info, Level};
use regex::Regex;
use clap::{Parser,CommandFactory};
use clap_complete::generate;

mod cli_parse;
use cli_parse::*;

type CmdMPSC = (AmiAction, Option<oneshot::Sender<Result<AmiResponse, AmiError>>>);

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Cli::parse();
    if let Commands::Completions { command_name, shell } = &args.command {
        generate(shell.clone(), &mut Cli::command(),
            command_name.clone().unwrap_or(std::env::args().next().unwrap()),
            &mut std::io::stdout());
        std::process::exit(0);
    }
    if let Commands::Server { system_name, host, port, ami_user, password } = &args.command {
        let options = ManagerOptions {
            port: *port,
            host: host.clone(),
            username: ami_user.clone(),
            password: password.clone(),
            events: true,
        };

        let mut manager = Manager::new();
        manager.connect_and_login(options).await?;
        info!("Successfully connected to AMI!");

        let mut events = manager.all_events_stream().await;
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<CmdMPSC>(100);
        let (event_tx, _) = broadcast::channel::<AmiEvent>(100);
        loop {
            select! {
                evx = events.next() => {
                    if let Some(Ok(ev)) = evx {
                        handle_event(ev, &cmd_tx, &event_tx,
                            system_name.clone()).await;
                    }
                }
                cmd = cmd_rx.recv() => {
                    if let Some((action, resp_tx_opt)) = cmd {
                        let res = manager.send_action(action).await;
                        if let Some(resp_tx) = resp_tx_opt {
                            resp_tx.send(res).unwrap();
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

async fn handle_event(ev: AmiEvent,
    cmd_tx: &mpsc::Sender<CmdMPSC>,
    event_tx: &broadcast::Sender<AmiEvent>,
    system_name: String) {

    if event_tx.receiver_count() != 0 {
        let _ = event_tx.send(ev.clone());
    }
    if let AmiEvent::UnknownEvent {event_type: t, fields: f} = ev {
        if t == "AsyncAGIStart" {
            let cmd_txc = cmd_tx.clone();
            let event_rxc = event_tx.subscribe();
            tokio::spawn(async move {
                let _ = async_agi_task(f, cmd_txc, event_rxc,
                    system_name.clone()).await;
            });
        }
    }
}

async fn async_agi_task(fs: HashMap<String, String>,
    cmd_tx: mpsc::Sender<CmdMPSC>,
    event_rx: broadcast::Receiver<AmiEvent>,
    system_name: String) -> Result<()> {

    let mut env: HashMap<String, String> = HashMap::new();
    for line in decode(fs.get("Env").ok_or(anyhow!("Missing key"))?)?.split("\n") {
        if let Some((k, v)) = line.split_once(": ") {
            env.insert(k.to_string(), v.to_string());
        }
    }
    if *env.get("agi_arg_1").ok_or(anyhow!("Missing system"))? != system_name {
        return Ok(())
    }
    let chan = fs.get("Channel").ok_or(anyhow!("Missing channel"))?.clone();
    let ca = ChannelAssociated {chan: chan.clone(), cmd_tx, event_rx};

    if let Some(arg2) = env.get("agi_arg_2") {
        debug!("{:40} → entrypt called with {}", chan, arg2);
        let res = match arg2.as_str() {
            "ringback" => ringback_task(ca, env, system_name).await,
            "ringback_playback" => ringback_playback_task(ca, env).await,
            _ => {Ok(())}
        };
        debug!("{:40} → Exited with {:?}", chan, res);
        res
    } else {Ok(())}
}

async fn ringback_task(mut ca: ChannelAssociated, env: HashMap<String, String>,
    system_name: String) -> Result<()> {

    debug!("{:40} → Starting ringback_task", ca.chan);
    let uniqueid = env.get("agi_uniqueid").ok_or(anyhow!("Cannot get uniqueid"))?.to_string();
    let callerid = env.get("agi_callerid").ok_or(anyhow!("Cannot get callerid"))?.to_string();
    if !Regex::new(r"[0-9]+")?.is_match(&callerid) {
        return Err(anyhow!("Invalid callerid"));
    }

    let do_rec = env.get("agi_arg_3") == Some(&"rec".to_string());

    ca.agi(String::from("answer")).await?;
    sleep(Duration::from_millis(1)).await;

    let time_r = if let Some(time_r) = env.get("agi_arg_4") {
        time_r
    } else {
        "000"
    };

    if do_rec {
        ca.agi(format!("exec Record /tmp/{}.ulaw,10,60,k", uniqueid)).await?;
    }
    ca.agi(String::from("exec PlayTones 500")).await?;
    loop {
        match ca.recv_pertinent_event().await {
            Ok(AmiEvent::Hangup(_)) => {
                break
            }
            Ok(_) => {},
            Err(broadcast::error::RecvError::Lagged(_)) => (),
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }

    if time_r != "" {
        let time_r_s = time_r.split_at_checked(time_r.len()-1)
            .ok_or(anyhow!("Time too short"))?;
        let time_mant = time_r_s.0.parse::<u64>()?;
        let time_exp  = time_r_s.1.parse::<u32>()?;
        let time_wait = Duration::from_secs(time_mant*10u64.pow(time_exp));
        debug!("{:40} → Waiting for {:?}", ca.chan, time_wait);
        sleep(time_wait).await;
        //let to = ca.chan.rsplit_once("-").ok_or(anyhow!("Cannot derive channel"))?.0.to_string();
        ca.cmd_tx.send((AmiAction::Custom {
                action: "Originate".to_string(),
                params: if do_rec {
                    HashMap::from([
                        ("Channel".to_string(), format!("Local/{}@from-internal", callerid)), // FIXME
                        ("Application".to_string(), "AGI".to_string()),
                        ("Data".to_string(), format!("agi:async,{},ringback_playback,{}", system_name, uniqueid)),
                    ])
                } else {
                    HashMap::from([
                        ("Channel".to_string(), format!("Local/{}@from-internal", callerid)), // FIXME
                        ("Application".to_string(), "AGI".to_string()),
                        ("Data".to_string(), format!("agi:async,{},ringback,,,", system_name)),
                    ])
                },
                action_id: None
            },
            None
        )).await?;
    }
    Ok(())
}

async fn ringback_playback_task(mut ca: ChannelAssociated, env: HashMap<String, String>) -> Result<()> {
    debug!("{:40} → Starting ringback_playback_task", ca.chan);
    let prev_uniqueid = env.get("agi_arg_3").ok_or(anyhow!("Cannot get previous-invocation uniqueid"))?.to_string();

    ca.agi(String::from("answer")).await?;
    sleep(Duration::from_millis(1)).await;
    for _ in 0..2 {
        ca.agi_wait(format!("exec PlayTones 800/100,0/300")).await?;
        ca.agi_wait(format!("exec Wait 2")).await?;
        ca.agi_wait(format!("exec StopPlayTones")).await?;
        ca.agi_wait(format!("exec Playback /tmp/{}", prev_uniqueid)).await?;
    }
    ca.agi(String::from("hangup")).await?;
    Ok(())
}

#[derive(Debug)]
struct ChannelAssociated {
    chan: String,
    cmd_tx: mpsc::Sender<CmdMPSC>,
    event_rx: broadcast::Receiver<AmiEvent>,
}

impl ChannelAssociated {
    async fn agi(&self, cmd: String) -> Result<(),
        mpsc::error::SendError<CmdMPSC>> {

        self.cmd_tx.send((AmiAction::Custom {
                action: "AGI".to_string(),
                params: HashMap::from([
                    ("Channel".to_string(), self.chan.clone()),
                    ("Command".to_string(), cmd)
                ]),
                action_id: None
            },
            None
        )).await
    }
    async fn agi_wait(&mut self, cmd: String) -> Result<()> {

        self.agi(cmd).await?;
        loop {
            match self.recv_pertinent_event().await {
                Ok(AmiEvent::Hangup(_)) => {
                    return Err(anyhow!("Hung up during command"));
                },
                Ok(AmiEvent::UnknownEvent {event_type, fields: _}) => {
                    if event_type == "AsyncAGIExec"
                    {
                        return Ok(());
                    }
                },
                Ok(_) => {},
                Err(broadcast::error::RecvError::Lagged(_)) => (),
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }
    async fn queue_digits(&mut self, num_digits: usize) -> Result<String> {
        let mut buf = String::new();
        while buf.len() < num_digits {
            match self.recv_pertinent_event().await {
                Ok(AmiEvent::Hangup(_)) => {
                },
                Ok(AmiEvent::UnknownEvent {event_type, fields}) => {
                    if event_type == "DTMFEnd" {
                        buf.push_str(fields.get("Digit").ok_or(anyhow!("Missing digit"))?)
                    }
                },
                Ok(_) => {},
                Err(_) => bail!("Cannot read event"),
            }
        }
        Ok(buf)
    }
    async fn recv_pertinent_event(&mut self) -> Result<AmiEvent,
        broadcast::error::RecvError> {

        loop {
            let event = self.event_rx.recv().await;
            match &event {
                Ok(AmiEvent::Hangup(HangupEventData { channel: hup_chan, .. })) if *hup_chan == self.chan => return event,
                Ok(AmiEvent::UnknownEvent { fields, .. }) => {
                    if let Some(event_chan) = fields.get("Channel") {
                        if self.chan == *event_chan {
                            return event;
                        }
                    } else {
                        return event
                    }
                },
                Ok(_) => {},
                Err(_) => {return event},
            }
        }
    }
}
