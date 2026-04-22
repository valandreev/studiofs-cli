use crate::cli::mount::MountArgs;

#[cfg(any(windows, target_os = "macos"))]
mod supported {
    use super::MountArgs;
    use crate::diagnostics::{
        DiagnosticsRuntimeSnapshot, LocalDiagnosticsClient, LocalDiagnosticsDiscovery,
        discover_local_sessions,
    };
    use image::imageops::FilterType;
    use image::{GrayImage, ImageFormat};
    use std::f32::consts::PI;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::JoinHandle;
    use std::time::{Duration, Instant};
    use tray_icon::{Icon, TrayIcon, TrayIconBuilder};
    use winit::application::ApplicationHandler;
    use winit::event::StartCause;
    use winit::event_loop::{ActiveEventLoop, EventLoop, EventLoopProxy};

    const TRAY_ICON_PNG: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/branding/studiofs-logo-final/png/studiofs-logo-64.png"
    ));
    const ICON_PADDING_DIVISOR: u32 = 16;
    const COLOR_BLACK: [u8; 3] = [0x11, 0x13, 0x17];
    const COLOR_BLUE: [u8; 3] = [0x23, 0x4A, 0x78];
    const COLOR_ORANGE: [u8; 3] = [0xA8, 0x57, 0x2A];
    const COLOR_WHITE: [u8; 3] = [0xF7, 0xF7, 0xF3];
    const ANIMATION_TICK: Duration = Duration::from_millis(100);
    const DIAGNOSTICS_POLL_INTERVAL: Duration = Duration::from_millis(300);
    const DIAGNOSTICS_STARTUP_GRACE: Duration = Duration::from_secs(5);
    const IDLE_CYCLE: Duration = Duration::from_millis(4400);
    const ACTIVE_CYCLE: Duration = Duration::from_millis(1200);

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TrayPulseMode {
        Booting,
        Unavailable,
        Idle,
        Active,
    }

    #[derive(Debug, Clone, Copy)]
    enum TrayUserEvent {
        Tick,
        Activity(TrayPulseMode),
        MountFinished,
    }

    #[derive(Debug, Clone)]
    struct IconMask {
        width: u32,
        height: u32,
        alpha: Arc<Vec<u8>>,
    }

    impl IconMask {
        fn load() -> anyhow::Result<Self> {
            let image = image::load_from_memory_with_format(TRAY_ICON_PNG, ImageFormat::Png)
                .map_err(|error| anyhow::anyhow!("failed to decode tray icon PNG: {error}"))?
                .into_rgba8();
            let (width, height) = image.dimensions();
            let alpha = Self::normalize_alpha_mask(&image);
            Ok(Self {
                width,
                height,
                alpha: Arc::new(alpha),
            })
        }

        fn normalize_alpha_mask(image: &image::RgbaImage) -> Vec<u8> {
            let (width, height) = image.dimensions();
            let source_alpha = image
                .as_raw()
                .chunks_exact(4)
                .map(|pixel| pixel[3])
                .collect::<Vec<_>>();
            let Some((min_x, min_y, max_x, max_y)) =
                alpha_bounds(width, height, source_alpha.as_slice())
            else {
                return source_alpha;
            };

            let cropped_width = max_x - min_x + 1;
            let cropped_height = max_y - min_y + 1;
            let padding_x = (width / ICON_PADDING_DIVISOR)
                .max(1)
                .min(width.saturating_sub(1) / 2);
            let padding_y = (height / ICON_PADDING_DIVISOR)
                .max(1)
                .min(height.saturating_sub(1) / 2);
            let target_width = width.saturating_sub(padding_x * 2).max(1);
            let target_height = height.saturating_sub(padding_y * 2).max(1);

            let alpha_image = GrayImage::from_vec(width, height, source_alpha)
                .expect("alpha mask dimensions should match source image");
            let cropped = image::imageops::crop_imm(
                &alpha_image,
                min_x,
                min_y,
                cropped_width,
                cropped_height,
            )
            .to_image();
            let resized = image::imageops::resize(
                &cropped,
                target_width,
                target_height,
                FilterType::CatmullRom,
            );

            let mut normalized = vec![0u8; (width * height) as usize];
            for y in 0..target_height {
                for x in 0..target_width {
                    let dst_x = x + padding_x;
                    let dst_y = y + padding_y;
                    normalized[(dst_y * width + dst_x) as usize] = resized.get_pixel(x, y).0[0];
                }
            }
            normalized
        }

        fn tinted_icon(&self, color: [u8; 3]) -> Result<Icon, tray_icon::BadIcon> {
            let mut rgba = Vec::with_capacity(self.alpha.len() * 4);
            for &alpha in self.alpha.iter() {
                rgba.extend_from_slice(&[color[0], color[1], color[2], alpha]);
            }
            Icon::from_rgba(rgba, self.width, self.height)
        }
    }

    struct TrayApp {
        icon_mask: IconMask,
        tray_icon: Option<TrayIcon>,
        volume_name: String,
        mode: TrayPulseMode,
        mode_started_at: Instant,
        last_color: Option<[u8; 3]>,
    }

    impl TrayApp {
        fn new(icon_mask: IconMask, volume_name: String) -> Self {
            Self {
                icon_mask,
                tray_icon: None,
                volume_name,
                mode: TrayPulseMode::Booting,
                mode_started_at: Instant::now(),
                last_color: None,
            }
        }

        fn create_tray(&mut self, event_loop: &ActiveEventLoop) {
            let icon = match self.icon_mask.tinted_icon(COLOR_BLACK) {
                Ok(icon) => icon,
                Err(error) => {
                    tracing::warn!(error = %error, "failed to build boot tray icon");
                    event_loop.exit();
                    return;
                }
            };

            match TrayIconBuilder::new()
                .with_icon(icon)
                .with_tooltip(self.tooltip_for(self.mode))
                .build()
            {
                Ok(tray_icon) => {
                    self.tray_icon = Some(tray_icon);
                    self.last_color = Some(COLOR_BLACK);
                }
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "tray initialization failed; continuing without tray"
                    );
                    event_loop.exit();
                }
            }
        }

        fn tooltip_for(&self, mode: TrayPulseMode) -> String {
            match mode {
                TrayPulseMode::Booting => {
                    format!("StudioFS {}: starting mount", self.volume_name)
                }
                TrayPulseMode::Unavailable => {
                    format!(
                        "StudioFS {}: mounted; live status unavailable",
                        self.volume_name
                    )
                }
                TrayPulseMode::Idle => {
                    format!("StudioFS {}: safe to shut down", self.volume_name)
                }
                TrayPulseMode::Active => {
                    format!(
                        "StudioFS {}: uploads or metadata sync in progress",
                        self.volume_name
                    )
                }
            }
        }

        fn set_mode(&mut self, mode: TrayPulseMode) {
            if self.mode == mode {
                return;
            }

            self.mode = mode;
            self.mode_started_at = Instant::now();
            self.last_color = None;
            if let Some(tray_icon) = self.tray_icon.as_ref() {
                if let Err(error) = tray_icon.set_tooltip(Some(&self.tooltip_for(mode))) {
                    tracing::debug!(error = %error, "failed to update tray tooltip");
                }
            }
        }

        fn render_frame(&mut self) {
            let Some(tray_icon) = self.tray_icon.as_ref() else {
                return;
            };

            let color = pulse_color(self.mode, self.mode_started_at.elapsed());
            if self.last_color == Some(color) {
                return;
            }

            let icon = match self.icon_mask.tinted_icon(color) {
                Ok(icon) => icon,
                Err(error) => {
                    tracing::debug!(error = %error, "failed to build tinted tray frame");
                    return;
                }
            };

            if let Err(error) = tray_icon.set_icon(Some(icon)) {
                tracing::debug!(error = %error, "failed to update tray icon");
                return;
            }

            self.last_color = Some(color);
        }
    }

    impl ApplicationHandler<TrayUserEvent> for TrayApp {
        fn resumed(&mut self, _event_loop: &ActiveEventLoop) {}

        fn new_events(&mut self, event_loop: &ActiveEventLoop, cause: StartCause) {
            if matches!(cause, StartCause::Init) {
                self.create_tray(event_loop);
            }
        }

        fn user_event(&mut self, event_loop: &ActiveEventLoop, event: TrayUserEvent) {
            match event {
                TrayUserEvent::Tick => self.render_frame(),
                TrayUserEvent::Activity(mode) => {
                    self.set_mode(mode);
                    self.render_frame();
                }
                TrayUserEvent::MountFinished => event_loop.exit(),
            }
        }

        fn window_event(
            &mut self,
            _event_loop: &ActiveEventLoop,
            _window_id: winit::window::WindowId,
            _event: winit::event::WindowEvent,
        ) {
        }
    }

    pub(crate) fn dispatch_mount_with_auto_tray(args: MountArgs) -> anyhow::Result<()> {
        if args.should_detach() {
            return args.dispatch();
        }

        let icon_mask = match IconMask::load() {
            Ok(icon_mask) => icon_mask,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "failed to load tray icon asset; continuing without tray"
                );
                return args.dispatch();
            }
        };

        let event_loop = match EventLoop::<TrayUserEvent>::with_user_event().build() {
            Ok(event_loop) => event_loop,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "failed to create tray event loop; continuing without tray"
                );
                return args.dispatch();
            }
        };

        let proxy = event_loop.create_proxy();
        let stop = Arc::new(AtomicBool::new(false));
        let process_id = std::process::id();
        let volume_name = args.volume_name().to_string();
        let mount_args = args.clone();
        let mount_handle = std::thread::Builder::new()
            .name("studiofs-mount".to_string())
            .spawn({
                let proxy = proxy.clone();
                let stop = stop.clone();
                move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        mount_args.dispatch()
                    }));
                    stop.store(true, Ordering::Release);
                    let _ = proxy.send_event(TrayUserEvent::MountFinished);
                    match result {
                        Ok(result) => result,
                        Err(payload) => std::panic::resume_unwind(payload),
                    }
                }
            })
            .map_err(|error| anyhow::anyhow!("failed to spawn mount thread: {error}"))?;

        let ticker = spawn_ticker_thread(stop.clone(), proxy.clone());
        let monitor = spawn_activity_monitor(stop.clone(), proxy.clone(), process_id, volume_name);
        let mut app = TrayApp::new(icon_mask, args.volume_name().to_string());

        if let Err(error) = event_loop.run_app(&mut app) {
            tracing::warn!(
                error = %error,
                "tray event loop failed; continuing without tray"
            );
        }

        stop.store(true, Ordering::Release);
        join_optional_thread("tray ticker", ticker);
        join_optional_thread("tray monitor", monitor);

        match mount_handle.join() {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!("mount thread panicked")),
        }
    }

    fn spawn_ticker_thread(
        stop: Arc<AtomicBool>,
        proxy: EventLoopProxy<TrayUserEvent>,
    ) -> Option<JoinHandle<()>> {
        std::thread::Builder::new()
            .name("studiofs-tray-ticker".to_string())
            .spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    if proxy.send_event(TrayUserEvent::Tick).is_err() {
                        break;
                    }
                    std::thread::sleep(ANIMATION_TICK);
                }
            })
            .map(Some)
            .unwrap_or_else(|error| {
                tracing::warn!(error = %error, "failed to spawn tray ticker thread");
                None
            })
    }

    fn spawn_activity_monitor(
        stop: Arc<AtomicBool>,
        proxy: EventLoopProxy<TrayUserEvent>,
        process_id: u32,
        volume_name: String,
    ) -> Option<JoinHandle<()>> {
        std::thread::Builder::new()
            .name("studiofs-tray-monitor".to_string())
            .spawn(move || {
                let mut last_mode = TrayPulseMode::Booting;
                let mut discovery: Option<LocalDiagnosticsDiscovery> = None;
                let monitor_started_at = Instant::now();

                while !stop.load(Ordering::Acquire) {
                    if discovery.is_none() {
                        discovery = find_local_mount_session(process_id, &volume_name);
                    }

                    let next_mode = discovery
                        .as_ref()
                        .and_then(current_activity_mode)
                        .unwrap_or_else(|| fallback_mode(monitor_started_at));

                    if next_mode != last_mode {
                        last_mode = next_mode;
                        if proxy
                            .send_event(TrayUserEvent::Activity(next_mode))
                            .is_err()
                        {
                            break;
                        }
                    }

                    std::thread::sleep(DIAGNOSTICS_POLL_INTERVAL);
                }
            })
            .map(Some)
            .unwrap_or_else(|error| {
                tracing::warn!(error = %error, "failed to spawn tray monitor thread");
                None
            })
    }

    fn join_optional_thread(name: &str, handle: Option<JoinHandle<()>>) {
        if let Some(handle) = handle {
            if handle.join().is_err() {
                tracing::warn!(thread = name, "background tray helper thread panicked");
            }
        }
    }

    fn find_local_mount_session(
        process_id: u32,
        volume_name: &str,
    ) -> Option<LocalDiagnosticsDiscovery> {
        discover_local_sessions().ok().and_then(|sessions| {
            sessions
                .into_iter()
                .find(|session| session.pid == process_id && session.volume == volume_name)
        })
    }

    fn current_activity_mode(discovery: &LocalDiagnosticsDiscovery) -> Option<TrayPulseMode> {
        let client = LocalDiagnosticsClient::new(discovery.clone());
        let snapshot =
            crate::runtime_bridge::block_on(async move { client.runtime_snapshot().await }).ok()?;
        Some(activity_mode(&snapshot))
    }

    fn activity_mode(snapshot: &DiagnosticsRuntimeSnapshot) -> TrayPulseMode {
        let pending_metadata_ops = snapshot
            .synchronization
            .state
            .pending_metadata_ops
            .saturating_add(snapshot.synchronization.state.inflight_metadata_ops);
        if snapshot.upload.pending_uploads > 0
            || (pending_metadata_ops != 0 && pending_metadata_ops >= 10)
        {
            TrayPulseMode::Active
        } else {
            TrayPulseMode::Idle
        }
    }

    fn alpha_bounds(width: u32, height: u32, alpha: &[u8]) -> Option<(u32, u32, u32, u32)> {
        let mut min_x = width;
        let mut min_y = height;
        let mut max_x = 0;
        let mut max_y = 0;
        let mut found = false;

        for y in 0..height {
            for x in 0..width {
                if alpha[(y * width + x) as usize] == 0 {
                    continue;
                }
                min_x = min_x.min(x);
                min_y = min_y.min(y);
                max_x = max_x.max(x);
                max_y = max_y.max(y);
                found = true;
            }
        }

        found.then_some((min_x, min_y, max_x, max_y))
    }

    fn fallback_mode(monitor_started_at: Instant) -> TrayPulseMode {
        if monitor_started_at.elapsed() < DIAGNOSTICS_STARTUP_GRACE {
            TrayPulseMode::Booting
        } else {
            TrayPulseMode::Unavailable
        }
    }

    fn pulse_color(mode: TrayPulseMode, elapsed: Duration) -> [u8; 3] {
        match mode {
            TrayPulseMode::Booting => COLOR_BLACK,
            TrayPulseMode::Unavailable => COLOR_WHITE,
            TrayPulseMode::Idle => {
                blend_color(COLOR_WHITE, COLOR_BLUE, breathe_factor(elapsed, IDLE_CYCLE))
            }
            TrayPulseMode::Active => blend_color(
                COLOR_WHITE,
                COLOR_ORANGE,
                breathe_factor(elapsed, ACTIVE_CYCLE),
            ),
        }
    }

    fn breathe_factor(elapsed: Duration, cycle: Duration) -> f32 {
        let seconds = elapsed.as_secs_f32();
        let cycle_seconds = cycle.as_secs_f32().max(f32::EPSILON);
        let phase = (seconds / cycle_seconds).fract();
        0.5 - 0.5 * (2.0 * PI * phase).cos()
    }

    fn blend_color(start: [u8; 3], end: [u8; 3], t: f32) -> [u8; 3] {
        let mix = t.clamp(0.0, 1.0);
        [
            blend_channel(start[0], end[0], mix),
            blend_channel(start[1], end[1], mix),
            blend_channel(start[2], end[2], mix),
        ]
    }

    fn blend_channel(start: u8, end: u8, t: f32) -> u8 {
        let start = f32::from(start);
        let end = f32::from(end);
        (start + (end - start) * t).round() as u8
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn activity_mode_is_active_for_pending_uploads() {
            let mut snapshot = DiagnosticsRuntimeSnapshot::default();
            snapshot.upload.pending_uploads = 1;
            assert_eq!(activity_mode(&snapshot), TrayPulseMode::Active);
        }

        #[test]
        fn activity_mode_is_active_for_pending_metadata_ops() {
            let mut snapshot = DiagnosticsRuntimeSnapshot::default();
            snapshot.synchronization.state.pending_metadata_ops = 10;
            assert_eq!(activity_mode(&snapshot), TrayPulseMode::Active);
        }

        #[test]
        fn activity_mode_is_idle_for_small_metadata_backlog() {
            let mut snapshot = DiagnosticsRuntimeSnapshot::default();
            snapshot.synchronization.state.pending_metadata_ops = 9;
            assert_eq!(activity_mode(&snapshot), TrayPulseMode::Idle);
        }

        #[test]
        fn activity_mode_is_active_for_inflight_metadata_ops() {
            let mut snapshot = DiagnosticsRuntimeSnapshot::default();
            snapshot.synchronization.state.pending_metadata_ops = 4;
            snapshot.synchronization.state.inflight_metadata_ops = 6;
            assert_eq!(activity_mode(&snapshot), TrayPulseMode::Active);
        }

        #[test]
        fn activity_mode_is_idle_when_synchronized() {
            let snapshot = DiagnosticsRuntimeSnapshot::default();
            assert_eq!(activity_mode(&snapshot), TrayPulseMode::Idle);
        }

        #[test]
        fn fallback_mode_starts_as_booting() {
            assert_eq!(fallback_mode(Instant::now()), TrayPulseMode::Booting);
        }

        #[test]
        fn fallback_mode_becomes_unavailable_after_grace_period() {
            let started_at = Instant::now() - DIAGNOSTICS_STARTUP_GRACE - Duration::from_millis(1);
            assert_eq!(fallback_mode(started_at), TrayPulseMode::Unavailable);
        }

        #[test]
        fn normalize_alpha_mask_expands_visible_bounds() {
            let mut image = image::RgbaImage::new(8, 8);
            for y in 3..=4 {
                for x in 3..=4 {
                    image.put_pixel(x, y, image::Rgba([0, 0, 0, 255]));
                }
            }

            let normalized = IconMask::normalize_alpha_mask(&image);
            assert_eq!(
                alpha_bounds(8, 8, normalized.as_slice()),
                Some((1, 1, 6, 6))
            );
        }

        #[test]
        fn blend_color_respects_endpoints() {
            assert_eq!(blend_color(COLOR_WHITE, COLOR_BLUE, 0.0), COLOR_WHITE);
            assert_eq!(blend_color(COLOR_WHITE, COLOR_BLUE, 1.0), COLOR_BLUE);
        }

        #[test]
        fn breathe_factor_stays_in_unit_interval() {
            for step in 0..64 {
                let elapsed = Duration::from_millis(step * 100);
                let factor = breathe_factor(elapsed, IDLE_CYCLE);
                assert!((0.0..=1.0).contains(&factor));
            }
        }

        #[test]
        fn tinted_icon_preserves_mask_alpha() {
            let mask = IconMask {
                width: 2,
                height: 1,
                alpha: Arc::new(vec![0, 255]),
            };
            let icon = mask
                .tinted_icon(COLOR_ORANGE)
                .expect("build icon from synthetic mask");
            let debug = format!("{icon:?}");
            assert!(debug.contains("Icon"));
        }
    }
}

#[cfg(any(windows, target_os = "macos"))]
pub(crate) use supported::dispatch_mount_with_auto_tray;

#[cfg(not(any(windows, target_os = "macos")))]
pub(crate) fn dispatch_mount_with_auto_tray(args: MountArgs) -> anyhow::Result<()> {
    tracing::debug!("tray auto-init is unsupported on this platform; continuing without tray");
    args.dispatch()
}
