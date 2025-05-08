# Hyper-V CPI Extension

This is a dynamic extension (DLL) implementation for managing Hyper-V virtual machines through the CPI (Cloud Provider Interface).

## Features

- Test Hyper-V installation
- List, create, delete, and manage virtual machines
- Disk volume management (create, list, attach, detach)
- Snapshot management
- Network configuration
- VM metadata management

## Requirements

- Windows operating system
- Hyper-V role installed and enabled
- PowerShell with Hyper-V module
- Administrative privileges

## Building

```bash
cargo build --release
```

The resulting DLL will be in `target/release/cpi_hyperv.dll`.

## Installation

Copy the DLL to your application's extensions directory.

## Usage

This extension implements the following actions:

### VM Management
- `test_install`: Test if Hyper-V is properly installed
- `list_workers`: List all virtual machines
- `create_worker`: Create a new virtual machine
- `delete_worker`: Delete a virtual machine
- `get_worker`: Get information about a virtual machine
- `has_worker`: Check if a virtual machine exists
- `start_worker`: Start a virtual machine
- `reboot_worker`: Reboot a virtual machine

### Disk Management
- `get_volumes`: List all virtual disk volumes
- `has_volume`: Check if a disk volume exists
- `create_volume`: Create a new disk volume
- `delete_volume`: Delete a disk volume
- `attach_volume`: Attach a disk to a VM
- `detach_volume`: Detach a disk from a VM
- `snapshot_volume`: Clone a disk volume

### Snapshot Management
- `create_snapshot`: Create a snapshot of a VM
- `delete_snapshot`: Delete a snapshot of a VM
- `has_snapshot`: Check if a snapshot exists

### Network & Configuration
- `configure_networks`: Configure network settings for a VM
- `set_worker_metadata`: Set metadata for a VM

## Technical Details

This extension uses PowerShell commands to interact with the Hyper-V API. All operations are performed by executing PowerShell scripts via the `Command` API in Rust.

### Implementation Notes

- Each VM (worker) must have a unique name
- The `id` field in responses generally uses the Hyper-V VM ID (GUID)
- For volumes, the full path is used as the ID
- The extension handles both single item and array return formats from PowerShell

## Security Considerations

Since this extension executes PowerShell commands, it requires appropriate permissions on the host system. It should be run with administrative privileges to ensure proper operation.

## Error Handling

All errors from PowerShell commands are captured and returned as structured error messages.