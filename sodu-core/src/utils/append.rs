// Copyright [2020] [Balaji Rajendran]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub trait SliceCopy<T: Copy> {
    fn copy_slice(&mut self, slice: &[T]);
}

impl<T: Copy> SliceCopy<T> for Vec<T> {
    fn copy_slice(&mut self, slice: &[T]) {
        if self.capacity() - self.len() < slice.len() {
            self.reserve(slice.len());
        }
        let bottom = self.len();
        unsafe { self.set_len(self.len() + slice.len()) }
        let top = bottom + slice.len();
        self[bottom..top].copy_from_slice(slice);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_slice_copy() {
        let mut buf = Vec::new();
        buf.copy_slice(&[1, 3, 3, 5]);
        assert_eq!(buf, vec![1, 3, 3, 5]);
        buf.copy_slice(&[2, 5]);
        assert_eq!(buf, vec![1, 3, 3, 5, 2, 5]);
    }
}
