use std::io;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use futures::StreamExt;
use librespot::connect::{ConnectConfig, Spirc};
use librespot::core::config::SessionConfig;
use librespot::core::session::Session;
use librespot::discovery::{DeviceType, Discovery};
use librespot::playback::audio_backend::{Sink, SinkError, SinkResult};
use librespot::playback::config::PlayerConfig;
use librespot::playback::convert::Converter;
use librespot::playback::decoder::AudioPacket;
use librespot::playback::mixer::{self, MixerConfig};
use librespot::playback::player::Player;
use librespot::playback::{NUM_CHANNELS, SAMPLE_RATE};

use super::{AudioSource, Format};

/// A Spotify Connect receiver. Advertises itself over Zeroconf; when a user
/// picks it in the Spotify app, librespot streams audio into a custom sink
/// that forwards f32 samples to [`LibrespotSource::next_samples`].
pub struct LibrespotSource {
    rx: Receiver<Vec<f32>>,
    // Kept alive for the process; the runtime thread ends when this source is
    // dropped and the channel closes.
    _runtime: thread::JoinHandle<()>,
}

impl LibrespotSource {
    /// Launch the receiver under the given device name (shown in Spotify).
    pub fn new(device_name: String) -> io::Result<Self> {
        let (tx, rx) = mpsc::channel::<Vec<f32>>();

        let runtime = thread::Builder::new()
            .name("librespot".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("build tokio runtime");
                rt.block_on(async move {
                    if let Err(e) = run_receiver(device_name, tx).await {
                        eprintln!("librespot receiver stopped: {e}");
                    }
                });
            })?;

        Ok(Self {
            rx,
            _runtime: runtime,
        })
    }
}

impl AudioSource for LibrespotSource {
    fn format(&self) -> Format {
        Format {
            channels: NUM_CHANNELS as u16,
            sample_rate: SAMPLE_RATE,
        }
    }

    fn next_samples(&mut self) -> io::Result<Option<Vec<f32>>> {
        // Blocks until the sink delivers the next block; `Err` (channel closed)
        // means the receiver thread ended -> end of stream.
        Ok(self.rx.recv().ok())
    }
    
    fn next_samples_timeout(&mut self, duration: Duration) -> io::Result<Option<Vec<f32>>> {
        // Blocks until the sink delivers the next block; `Err` (channel closed)
        // means the receiver thread ended -> end of stream.
        Ok(self.rx.recv_timeout(duration).ok())
    }
}

/// Advertise over Zeroconf and, for each set of credentials a controller hands
/// us, run a Connect session until it disconnects, then wait for the next.
async fn run_receiver(
    device_name: String,
    tx: Sender<Vec<f32>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let session_config = SessionConfig::default();

    let mut discovery = Discovery::builder(
        session_config.device_id.clone(),
        session_config.client_id.clone(),
    )
    .name(device_name.clone())
    .device_type(DeviceType::Speaker)
    .launch()?;

    println!("Spotify Connect receiver '{device_name}' is discoverable.");

    while let Some(credentials) = discovery.next().await {
        let session = Session::new(session_config.clone(), None);

        let mixer = match mixer::find(None) {
            Some(mk) => mk(MixerConfig::default())?,
            None => return Err("no mixer available".into()),
        };

        let sink_tx = tx.clone();
        let player = Player::new(
            PlayerConfig::default(),
            session.clone(),
            mixer.get_soft_volume(),
            move || Box::new(ChannelSink::new(sink_tx)),
        );

        let connect_config = ConnectConfig {
            name: device_name.clone(),
            device_type: DeviceType::Speaker,
            ..Default::default()
        };

        let (_spirc, spirc_task) =
            Spirc::new(connect_config, session, credentials, player, mixer).await?;

        // Runs until the controller disconnects this session; then we loop and
        // wait for the device to be selected again.
        spirc_task.await;
    }

    Ok(())
}

/// A librespot [`Sink`] that converts decoded samples to f32 and forwards them
/// over a channel. librespot writes packets as fast as it decodes and relies on
/// the sink blocking to pace playback (a real audio backend blocks on the
/// device). We have no such device here, so the sink paces itself to realtime;
/// otherwise librespot would decode an entire track instantly.
struct ChannelSink {
    tx: Sender<Vec<f32>>,
    // The wall-clock time by which the next packet should have been emitted.
    // `None` before the first packet and after a stop, so pacing restarts
    // cleanly across pauses/seeks rather than trying to "catch up".
    next_deadline: Option<Instant>,
}

impl ChannelSink {
    fn new(tx: Sender<Vec<f32>>) -> Self {
        Self {
            tx,
            next_deadline: None,
        }
    }
}

impl Sink for ChannelSink {
    fn start(&mut self) -> SinkResult<()> {
        // Reset the pacing clock so a resume/seek doesn't try to catch up.
        self.next_deadline = None;
        Ok(())
    }

    fn stop(&mut self) -> SinkResult<()> {
        self.next_deadline = None;
        Ok(())
    }

    fn write(&mut self, packet: AudioPacket, converter: &mut Converter) -> SinkResult<()> {
        let samples = match packet {
            AudioPacket::Samples(s) => converter.f64_to_f32(&s),
            // We never request raw/passthrough output.
            AudioPacket::Raw(_) => return Ok(()),
        };
        if samples.is_empty() {
            return Ok(());
        }

        // Pace to realtime: emit this packet no sooner than the running
        // deadline, then advance the deadline by the packet's own duration.
        let frames = samples.len() / NUM_CHANNELS as usize;
        let packet_dur = Duration::from_secs_f64(frames as f64 / SAMPLE_RATE as f64);
        let now = Instant::now();
        let deadline = match self.next_deadline {
            Some(d) if d > now => {
                thread::sleep(d - now);
                d
            }
            // First packet, or we've fallen behind (e.g. just resumed): emit
            // now and re-anchor the clock to avoid a burst.
            _ => now,
        };
        self.next_deadline = Some(deadline + packet_dur);

        self.tx
            .send(samples)
            .map_err(|_| SinkError::OnWrite("player disconnected".into()))
    }
}
