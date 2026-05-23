pub(super) struct SendMutBuffer {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: The wrapper only marks transport capability across threads.
// Call sites must guarantee pointer validity, exclusivity, and lifetime.
unsafe impl Send for SendMutBuffer {}

impl SendMutBuffer {
    pub(super) fn from_slice(slice: &mut [u8]) -> Self {
        Self {
            ptr: slice.as_mut_ptr(),
            len: slice.len(),
        }
    }

    /// # Safety
    ///
    /// `self.ptr` must be valid, properly aligned, and point to `self.len`
    /// initialised bytes. The caller must ensure exclusive access to the
    /// backing memory for the entire lifetime `'a`.
    pub(super) unsafe fn into_mut_slice<'a>(self) -> &'a mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

#[derive(Copy, Clone)]
pub(super) struct SendConstBuffer {
    ptr: *const u8,
    len: usize,
}

// SAFETY: The wrapper only marks transport capability across threads.
// Call sites must guarantee pointer validity and lifetime.
unsafe impl Send for SendConstBuffer {}

impl SendConstBuffer {
    pub(super) fn from_slice(slice: &[u8]) -> Self {
        Self {
            ptr: slice.as_ptr(),
            len: slice.len(),
        }
    }

    /// # Safety
    ///
    /// `self.ptr` must be valid, properly aligned, and point to `self.len`
    /// initialised bytes. The caller must keep the backing memory alive for
    /// the entire lifetime `'a`.
    pub(super) unsafe fn as_slice<'a>(self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}
