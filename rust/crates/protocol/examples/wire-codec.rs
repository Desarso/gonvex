use gonvex_protocol::{binary_replica::BinaryReplicaCodec, ServerMessage};
use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering};

struct TrackingAllocator;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) };
    }
}

fn main() {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input).unwrap();
    if std::env::args().any(|v| v == "decode") {
        let frames: Vec<Vec<u8>> = serde_json::from_str(&input).unwrap();
        let mut codec = BinaryReplicaCodec::default();
        let messages: Vec<serde_json::Value> = frames
            .iter()
            .map(|v| codec.decode_value(v).unwrap())
            .collect();
        println!("{}", serde_json::to_string(&messages).unwrap());
        return;
    }
    let messages: Vec<ServerMessage> = serde_json::from_str(&input).unwrap();
    let mut json_times = Vec::new();
    let mut binary_times = Vec::new();
    let mut output = Vec::new();
    let mut json_peak = 0;
    let mut binary_peak = 0;
    for iteration in 0..15 {
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        let start = std::time::Instant::now();
        let json: Vec<String> = messages
            .iter()
            .map(|m| serde_json::to_string(m).unwrap())
            .collect();
        std::hint::black_box(&json);
        json_times.push(start.elapsed().as_secs_f64() * 1000.0);
        json_peak = json_peak.max(PEAK.load(Ordering::Relaxed).saturating_sub(baseline));
        drop(json);
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        let start = std::time::Instant::now();
        let mut codec = BinaryReplicaCodec::default();
        let encoded: Vec<Vec<u8>> = messages.iter().map(|m| codec.encode(m).unwrap()).collect();
        binary_times.push(start.elapsed().as_secs_f64() * 1000.0);
        binary_peak = binary_peak.max(PEAK.load(Ordering::Relaxed).saturating_sub(baseline));
        if iteration == 14 {
            output = encoded;
        }
    }
    println!(
        "{}",
        serde_json::json!({"frames":output,"jsonMs":json_times,"binaryMs":binary_times,"jsonPeakBytes":json_peak,"binaryPeakBytes":binary_peak})
    );
}
