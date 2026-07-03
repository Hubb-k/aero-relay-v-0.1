use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub const BUF_SIZE: usize = 4096 * 1024;

pub struct BufferTask {
    data: Box<[u8; BUF_SIZE]>,
    write_ptr: AtomicUsize,
    last_update: AtomicUsize,
    write_lock: Mutex<()>,
}

impl BufferTask {
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize;

        let instance = Self {
            data: Box::new([0u8; BUF_SIZE]),
            write_ptr: AtomicUsize::new(0),
            last_update: AtomicUsize::new(now),
            write_lock: Mutex::new(()),
        };

        instance.reset();
        instance
    }

    pub fn reset(&self) {
        let _guard = self.write_lock.lock().unwrap();
        self.write_ptr.store(0, Ordering::SeqCst);
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as usize;
        self.last_update.store(now, Ordering::SeqCst);
        let p = self.data.as_ptr() as *mut u8;
        unsafe {
            std::ptr::write_bytes(p, 0, BUF_SIZE);
        }
    }

    pub fn push(&self, chunk: &[u8]) {
        let n = chunk.len();
        if n == 0 || n > BUF_SIZE { return; }

        let _guard = self.write_lock.lock().unwrap();
        let wp = self.write_ptr.load(Ordering::Relaxed);
        let p = self.data.as_ptr() as *mut u8;

        unsafe {
            if wp + n <= BUF_SIZE {
                std::ptr::copy_nonoverlapping(chunk.as_ptr(), p.add(wp), n);
            } else {
                let first_part = BUF_SIZE - wp;
                std::ptr::copy_nonoverlapping(chunk.as_ptr(), p.add(wp), first_part);
                std::ptr::copy_nonoverlapping(chunk.as_ptr().add(first_part), p, n - first_part);
            }
        }

        self.write_ptr.store((wp + n) % BUF_SIZE, Ordering::Release);
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as usize;
        self.last_update.store(now, Ordering::Release);
    }

    pub fn pull_from(&self, rp: usize) -> (Vec<u8>, usize) {
        let wp = self.write_ptr.load(Ordering::Acquire);
        let mut actual_rp = rp;
        if actual_rp >= BUF_SIZE {
            actual_rp = 0;
        }

        if actual_rp == wp {
            return (vec![], wp);
        }

        let mut res = Vec::new();
        if actual_rp < wp {
            res.extend_from_slice(&self.data[actual_rp..wp]);
        } else {
            res.extend_from_slice(&self.data[actual_rp..BUF_SIZE]);
            res.extend_from_slice(&self.data[0..wp]);
        }
        (res, wp)
    }

    pub fn get_current_head(&self) -> usize {
        self.write_ptr.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub fn is_dead(&self) -> bool {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as usize;
        now - self.last_update.load(Ordering::Relaxed) >= 3
    }
}
