#!/bin/bash
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