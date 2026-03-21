use clap::ValueEnum;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum MountProfileMode {
    Pyroscope,
    Off,
}
