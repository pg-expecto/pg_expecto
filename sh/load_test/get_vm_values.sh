#!/bin/bash
# Copyright 2026 Ринат (pg_expecto)
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# get_vm_values.sh
# version 6.0
echo "=== Параметры dirty pages ==="
for param in vm.dirty_background_ratio vm.dirty_ratio vm.dirty_background_bytes \
             vm.dirty_bytes vm.dirty_expire_centisecs vm.dirty_writeback_centisecs; do
    printf "%-35s: " "$param"
    sysctl -n $param 2>/dev/null || echo "N/A"
done

echo -e "\n=== Другие параметры VM ==="
for param in vm.vfs_cache_pressure vm.swappiness; do
    printf "%-35s: " "$param"
    sysctl -n $param 2>/dev/null || echo "N/A"
done

exit 0 
