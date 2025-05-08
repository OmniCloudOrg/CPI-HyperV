// File: cpi_hyperv/src/lib.rs
use lib_cpi::{
    ActionParameter, ActionDefinition, ActionResult, CpiExtension, ParamType,
    action, param, validation
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::process::Command;
use std::sync::Once;

#[unsafe(no_mangle)]
pub extern "C" fn get_extension() -> *mut dyn CpiExtension {
    Box::into_raw(Box::new(HyperVExtension::new()))
}

/// HyperV provider implemented as a dynamic extension
pub struct HyperVExtension {
    name: String,
    provider_type: String,
    default_settings: HashMap<String, Value>,
}

// Static initialization to warm up PowerShell on first use
static INIT: Once = Once::new();

impl HyperVExtension {
    pub fn new() -> Self {
        // Initialize PowerShell once for faster subsequent calls
        INIT.call_once(|| {
            // Warm up PowerShell with a simple command
            let warmup = Command::new("powershell.exe")
                .args([
                    "-NoLogo", 
                    "-NoProfile", 
                    "-NonInteractive", 
                    "-Command", 
                    "Write-Host 'Warming up PowerShell'"
                ])
                .output();
                
            if let Ok(_) = warmup {
                println!("PowerShell session warmed up");
            }
        });
        
        let mut default_settings = HashMap::new();
        default_settings.insert("memory_mb".to_string(), json!(2048));
        default_settings.insert("cpu_count".to_string(), json!(2));
        default_settings.insert("switch_name".to_string(), json!("Default Switch"));
        default_settings.insert("generation".to_string(), json!(2));
        default_settings.insert("username".to_string(), json!("Administrator"));
        default_settings.insert("password".to_string(), json!("password"));

        Self {
            name: "hyperv".to_string(),
            provider_type: "command".to_string(),
            default_settings,
        }
    }
    
    // Helper method to run PowerShell commands - optimized version
    fn run_powershell(&self, script: &str) -> Result<String, String> {
        println!("Running PowerShell script: {}", script);
        
        // Use PowerShell Core (pwsh) if available, as it has faster startup time
        // Fall back to regular PowerShell if pwsh is not found
        let command = if cfg!(windows) {
            "powershell.exe"
        } else {
            // Fallback for non-Windows (should not happen for Hyper-V)
            "powershell"
        };
        
        // Format the script with progress preference - create the string outside the if block
        let formatted_script = format!("& {{ $ProgressPreference = 'SilentlyContinue'; {} }}", script);
        
        let mut args = vec![
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy", "Bypass",
        ];
        
        // Add Windows-specific args
        if cfg!(windows) {
            args.push("-WindowStyle");
            args.push("Hidden");
        }
        
        // Add the command
        args.push("-Command");
        args.push(&formatted_script);
        
        let output = Command::new(command)
            .args(&args)
            .output()
            .map_err(|e| format!("Failed to execute PowerShell command: {}", e))?;
            
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            Ok(stdout)
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            Err(format!("PowerShell command failed: {}", stderr))
        }
    }
    
    // Implementation of individual actions
    
    fn test_install(&self) -> ActionResult {
        let script = "$PSVersionTable.PSVersion | ConvertTo-Json; \
                      Get-Command -Module Hyper-V | Measure-Object | Select-Object -ExpandProperty Count";
        
        let output = self.run_powershell(script)?;
        
        // Parse the output
        if let Some(version_line) = output.lines().next() {
            if version_line.contains("Major") {
                return Ok(json!({
                    "success": true,
                    "version": version_line.trim()
                }));
            }
        }
        
        Err("Could not determine PowerShell version".to_string())
    }
    
    fn list_workers(&self) -> ActionResult {
        // Use a more optimized PowerShell script with faster output format
        // Use CSV format which parses faster than JSON
        let script = "Get-VM | Select-Object Name, Id, State | ForEach-Object { \
                      $state = switch($_.State) { \
                        2 {'Running'} \
                        3 {'Stopped'} \
                        default {'Unknown'} \
                      }; \
                      [PSCustomObject]@{ \
                        Name=$_.Name; \
                        Id=$_.Id; \
                        State=$state \
                      } \
                    } | ConvertTo-Csv -NoTypeInformation";
                      
        let output = self.run_powershell(script)?;
        
        // Parse CSV output which is faster than JSON parsing
        let mut workers = Vec::new();
        let mut lines = output.lines();
        
        // Skip the header line
        if let Some(_header) = lines.next() {
            for line in lines {
                if line.trim().is_empty() {
                    continue;
                }
                
                // Parse CSV line
                let parts: Vec<&str> = line.split(',').collect();
                if parts.len() >= 3 {
                    // Trim quotes from CSV values
                    let name = parts[0].trim_matches('"').to_string();
                    let id = parts[1].trim_matches('"').to_string();
                    let state = parts[2].trim_matches('"').to_string();
                    
                    workers.push(json!({
                        "name": name,
                        "id": id,
                        "state": state
                    }));
                }
            }
        }
        
        Ok(json!({
            "workers": workers
        }))
    }
    
    fn create_worker(&self, worker_name: String, memory_mb: i64, cpu_count: i64, generation: i64, switch_name: String) -> ActionResult {
        // First, check if VM already exists
        let check_script = format!("Get-VM -Name \"{}\" -ErrorAction SilentlyContinue", worker_name);
        let check_output = self.run_powershell(&check_script);
        
        if let Ok(output) = check_output {
            if !output.trim().is_empty() {
                return Err(format!("VM '{}' already exists", worker_name));
            }
        }
        
        // Create VM
        let create_script = format!(
            "New-VM -Name \"{}\" -MemoryStartupBytes {}MB -Generation {} -SwitchName \"{}\" | Out-Null; \
             Set-VM -Name \"{}\" -ProcessorCount {}; \
             Get-VM -Name \"{}\" | Select-Object Name, Id, State | ConvertTo-Json",
            worker_name, memory_mb, generation, switch_name, worker_name, cpu_count, worker_name
        );
        
        let output = self.run_powershell(&create_script)?;
        
        // Parse the output JSON
        let vm_info: Value = serde_json::from_str(&output)
            .map_err(|e| format!("Failed to parse VM info: {}", e))?;
        
        let id = vm_info["Id"].as_str().unwrap_or("unknown").to_string();
        
        Ok(json!({
            "success": true,
            "id": id,
            "name": worker_name
        }))
    }
    
    fn delete_worker(&self, worker_name: String) -> ActionResult {
        // Stop VM if running
        let stop_script = format!(
            "Stop-VM -Name \"{}\" -TurnOff -Force -ErrorAction SilentlyContinue",
            worker_name
        );
        let _ = self.run_powershell(&stop_script);
        
        // Delete VM
        let delete_script = format!(
            "Remove-VM -Name \"{}\" -Force",
            worker_name
        );
        
        self.run_powershell(&delete_script)?;
        
        Ok(json!({
            "success": true
        }))
    }
    
    fn get_worker(&self, worker_name: String) -> ActionResult {
        let script = format!(
            "$vm = Get-VM -Name \"{}\" -ErrorAction Stop; \
             $vmInfo = $vm | Select-Object Name, Id, State, @{{Name='memory_mb';Expression={{$_.MemoryStartup / 1MB}}}}, \
             @{{Name='cpu_count';Expression={{$_.ProcessorCount}}}}, \
             @{{Name='generation';Expression={{$_.Generation}}}}; \
             $vmInfo | ConvertTo-Json",
            worker_name
        );
        
        let output = self.run_powershell(&script)?;
        
        // Parse the output JSON
        let vm_info: Value = serde_json::from_str(&output)
            .map_err(|e| format!("Failed to parse VM info: {}", e))?;
        
        let state = vm_info["State"].as_i64().map(|s| match s {
            2 => "Running",
            3 => "Stopped",
            _ => "Unknown"
        }).unwrap_or("Unknown").to_string();
        
        let result = json!({
            "success": true,
            "vm": {
                "name": vm_info["Name"].as_str().unwrap_or("unknown"),
                "id": vm_info["Id"].as_str().unwrap_or("unknown"),
                "state": state,
                "memory_mb": vm_info["memory_mb"].as_i64().unwrap_or(0),
                "cpu_count": vm_info["cpu_count"].as_i64().unwrap_or(0),
                "generation": vm_info["generation"].as_i64().unwrap_or(0)
            }
        });
        
        Ok(result)
    }
    
    fn has_worker(&self, worker_name: String) -> ActionResult {
        let script = format!(
            "Get-VM -Name \"{}\" -ErrorAction SilentlyContinue | Measure-Object | Select-Object -ExpandProperty Count",
            worker_name
        );
        
        let output = self.run_powershell(&script)?;
        
        let count = output.trim().parse::<i32>().unwrap_or(0);
        
        Ok(json!({
            "success": true,
            "exists": count > 0
        }))
    }
    
    fn start_worker(&self, worker_name: String) -> ActionResult {
        let script = format!(
            "Start-VM -Name \"{}\"",
            worker_name
        );
        
        self.run_powershell(&script)?;
        
        Ok(json!({
            "success": true,
            "started": worker_name
        }))
    }
    
    fn get_volumes(&self) -> ActionResult {
        let script = "$vhds = Get-VHD; $vhds | Select-Object Path, VhdType, Size, @{Name='SizeGB';Expression={$_.Size / 1GB}} | ConvertTo-Json";
        
        let output = self.run_powershell(script)?;
        
        // Parse the output
        let mut volumes = Vec::new();
        
        // Handle single disk case
        if output.trim().starts_with('{') {
            let disk_json: Result<Value, _> = serde_json::from_str(&output);
            if let Ok(disk) = disk_json {
                let path = disk["Path"].as_str().unwrap_or("unknown").to_string();
                let size_bytes = disk["Size"].as_i64().unwrap_or(0);
                let size_mb = size_bytes / (1024 * 1024);
                let vhd_type = disk["VhdType"].as_i64().map(|t| match t {
                    1 => "FixedSize",
                    2 => "DynamicExpanding",
                    3 => "Differencing",
                    _ => "Unknown"
                }).unwrap_or("Unknown").to_string();
                
                volumes.push(json!({
                    "id": path.clone(),
                    "path": path,
                    "size_mb": size_mb,
                    "format": vhd_type
                }));
            }
        } else if output.trim().starts_with('[') {
            let disks_json: Result<Vec<Value>, _> = serde_json::from_str(&output);
            if let Ok(disks) = disks_json {
                for disk in disks {
                    let path = disk["Path"].as_str().unwrap_or("unknown").to_string();
                    let size_bytes = disk["Size"].as_i64().unwrap_or(0);
                    let size_mb = size_bytes / (1024 * 1024);
                    let vhd_type = disk["VhdType"].as_i64().map(|t| match t {
                        1 => "FixedSize",
                        2 => "DynamicExpanding",
                        3 => "Differencing",
                        _ => "Unknown"
                    }).unwrap_or("Unknown").to_string();
                    
                    volumes.push(json!({
                        "id": path.clone(),
                        "path": path,
                        "size_mb": size_mb,
                        "format": vhd_type
                    }));
                }
            }
        }
        
        Ok(json!({
            "success": true,
            "volumes": volumes
        }))
    }
    
    fn has_volume(&self, disk_path: String) -> ActionResult {
        let script = format!(
            "Test-Path -Path \"{}\" -PathType Leaf",
            disk_path
        );
        
        let output = self.run_powershell(&script)?;
        
        let exists = output.trim().to_lowercase() == "true";
        
        Ok(json!({
            "success": true,
            "exists": exists
        }))
    }
    
    fn create_volume(&self, disk_path: String, size_mb: i64) -> ActionResult {
        let script = format!(
            "New-VHD -Path \"{}\" -SizeBytes {}MB -Dynamic; \
             Get-VHD -Path \"{}\" | Select-Object Path | ConvertTo-Json",
            disk_path, size_mb, disk_path
        );
        
        let output = self.run_powershell(&script)?;
        
        // Parse output to get the path
        let disk_json: Result<Value, _> = serde_json::from_str(&output.trim());
        
        match disk_json {
            Ok(disk) => {
                let path = disk["Path"].as_str().unwrap_or(&disk_path).to_string();
                
                Ok(json!({
                    "success": true,
                    "id": path.clone(),
                    "path": path
                }))
            },
            Err(_) => {
                // Fallback if we can't parse the JSON
                Ok(json!({
                    "success": true,
                    "id": disk_path.clone(),
                    "path": disk_path
                }))
            }
        }
    }
    
    fn delete_volume(&self, disk_path: String) -> ActionResult {
        let script = format!(
            "Remove-Item -Path \"{}\" -Force",
            disk_path
        );
        
        self.run_powershell(&script)?;
        
        Ok(json!({
            "success": true
        }))
    }
    
    fn attach_volume(&self, worker_name: String, controller_type: String, disk_path: String) -> ActionResult {
        // Determine controller type - supports IDE, SCSI, or DVD
        let controller_script = match controller_type.to_lowercase().as_str() {
            "ide" => format!(
                "Add-VMHardDiskDrive -VMName \"{}\" -Path \"{}\" -ControllerType IDE",
                worker_name, disk_path
            ),
            "dvd" => format!(
                "Add-VMDvdDrive -VMName \"{}\" -Path \"{}\"",
                worker_name, disk_path
            ),
            _ => format!(
                "Add-VMHardDiskDrive -VMName \"{}\" -Path \"{}\" -ControllerType SCSI",
                worker_name, disk_path
            ),
        };
        
        self.run_powershell(&controller_script)?;
        
        Ok(json!({
            "success": true
        }))
    }
    
    fn detach_volume(&self, worker_name: String, controller_type: String, disk_path: String) -> ActionResult {
        // Find the disk to remove
        let script = match controller_type.to_lowercase().as_str() {
            "dvd" => format!(
                "$drive = Get-VMDvdDrive -VMName \"{}\" | Where-Object {{ $_.Path -eq \"{}\" }}; \
                 if ($drive) {{ Remove-VMDvdDrive -VMDvdDrive $drive }}",
                worker_name, disk_path
            ),
            _ => format!(
                "$drive = Get-VMHardDiskDrive -VMName \"{}\" | Where-Object {{ $_.Path -eq \"{}\" }}; \
                 if ($drive) {{ Remove-VMHardDiskDrive -VMHardDiskDrive $drive }}",
                worker_name, disk_path
            ),
        };
        
        self.run_powershell(&script)?;
        
        Ok(json!({
            "success": true
        }))
    }
    
    fn create_snapshot(&self, worker_name: String, snapshot_name: String) -> ActionResult {
        let script = format!(
            "Checkpoint-VM -Name \"{}\" -SnapshotName \"{}\" | Select-Object Id | ConvertTo-Json",
            worker_name, snapshot_name
        );
        
        let output = self.run_powershell(&script)?;
        
        // Parse the checkpoint ID
        let snapshot_json: Result<Value, _> = serde_json::from_str(&output.trim());
        
        match snapshot_json {
            Ok(snapshot) => {
                let id = snapshot["Id"].as_str().unwrap_or("unknown").to_string();
                
                Ok(json!({
                    "success": true,
                    "id": id
                }))
            },
            Err(_) => {
                // Fallback if we can't parse the JSON
                Ok(json!({
                    "success": true,
                    "id": format!("{}-{}", worker_name, snapshot_name)
                }))
            }
        }
    }
    
    fn delete_snapshot(&self, worker_name: String, snapshot_name: String) -> ActionResult {
        let script = format!(
            "Remove-VMSnapshot -VMName \"{}\" -Name \"{}\" -IncludeAllChildSnapshots",
            worker_name, snapshot_name
        );
        
        self.run_powershell(&script)?;
        
        Ok(json!({
            "success": true
        }))
    }
    
    fn has_snapshot(&self, worker_name: String, snapshot_name: String) -> ActionResult {
        let script = format!(
            "Get-VMSnapshot -VMName \"{}\" -Name \"{}\" -ErrorAction SilentlyContinue | Measure-Object | Select-Object -ExpandProperty Count",
            worker_name, snapshot_name
        );
        
        let output = self.run_powershell(&script)?;
        
        let count = output.trim().parse::<i32>().unwrap_or(0);
        
        Ok(json!({
            "success": true,
            "exists": count > 0
        }))
    }
    
    fn reboot_worker(&self, worker_name: String) -> ActionResult {
        let script = format!(
            "Restart-VM -Name \"{}\" -Force",
            worker_name
        );
        
        self.run_powershell(&script)?;
        
        Ok(json!({
            "success": true
        }))
    }
    
    fn configure_networks(&self, worker_name: String, switch_name: String) -> ActionResult {
        let script = format!(
            "Get-VMNetworkAdapter -VMName \"{}\" | Connect-VMNetworkAdapter -SwitchName \"{}\"",
            worker_name, switch_name
        );
        
        self.run_powershell(&script)?;
        
        Ok(json!({
            "success": true
        }))
    }
    
    fn set_worker_metadata(&self, worker_name: String, key: String, value: String) -> ActionResult {
        // Hyper-V doesn't have a native metadata system, so we'll use Notes
        let script = format!(
            "$vm = Get-VM -Name \"{}\"; \
             $currentNotes = $vm.Notes; \
             $newNotes = if ($currentNotes) {{ \"$currentNotes`n{}={}\"; }} else {{ \"{}={}\"; }}; \
             Set-VM -Name \"{}\" -Notes $newNotes",
            worker_name, key, value, key, value, worker_name
        );
        
        self.run_powershell(&script)?;
        
        Ok(json!({
            "success": true
        }))
    }
    
    fn snapshot_volume(&self, source_volume_path: String, target_volume_path: String) -> ActionResult {
        let script = format!(
            "Convert-VHD -Path \"{}\" -DestinationPath \"{}\" -VHDType Differencing; \
             Get-VHD -Path \"{}\" | Select-Object Path | ConvertTo-Json",
            source_volume_path, target_volume_path, target_volume_path
        );
        
        let output = self.run_powershell(&script)?;
        
        let disk_json: Result<Value, _> = serde_json::from_str(&output.trim());
        
        match disk_json {
            Ok(disk) => {
                let path = disk["Path"].as_str().unwrap_or(&target_volume_path).to_string();
                
                Ok(json!({
                    "success": true,
                    "id": path.clone(),
                    "path": path
                }))
            },
            Err(_) => {
                // Fallback if we can't parse the JSON
                Ok(json!({
                    "success": true,
                    "id": target_volume_path.clone(),
                    "path": target_volume_path
                }))
            }
        }
    }
}

impl CpiExtension for HyperVExtension {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn provider_type(&self) -> &str {
        &self.provider_type
    }
    
    fn list_actions(&self) -> Vec<String> {
        vec![
            "test_install".to_string(),
            "list_workers".to_string(),
            "create_worker".to_string(),
            "delete_worker".to_string(),
            "get_worker".to_string(),
            "has_worker".to_string(),
            "start_worker".to_string(),
            "get_volumes".to_string(),
            "has_volume".to_string(),
            "create_volume".to_string(),
            "delete_volume".to_string(),
            "attach_volume".to_string(),
            "detach_volume".to_string(),
            "create_snapshot".to_string(),
            "delete_snapshot".to_string(),
            "has_snapshot".to_string(),
            "reboot_worker".to_string(),
            "configure_networks".to_string(),
            "set_worker_metadata".to_string(),
            "snapshot_volume".to_string()
        ]
    }
    
    fn get_action_definition(&self, action: &str) -> Option<ActionDefinition> {
        match action {
            "test_install" => Some(ActionDefinition {
                name: "test_install".to_string(),
                description: "Test if Hyper-V is properly installed".to_string(),
                parameters: vec![],
            }),
            "list_workers" => Some(ActionDefinition {
                name: "list_workers".to_string(),
                description: "List all virtual machines".to_string(),
                parameters: vec![],
            }),
            "create_worker" => Some(ActionDefinition {
                name: "create_worker".to_string(),
                description: "Create a new virtual machine".to_string(),
                parameters: vec![
                    param!("worker_name", "Name of the VM to create", ParamType::String, required),
                    param!("memory_mb", "Memory in MB", ParamType::Integer, optional, json!(2048)),
                    param!("cpu_count", "Number of CPUs", ParamType::Integer, optional, json!(2)),
                    param!("generation", "VM generation (1 or 2)", ParamType::Integer, optional, json!(2)),
                    param!("switch_name", "Network switch to connect to", ParamType::String, optional, json!("Default Switch")),
                ],
            }),
            "delete_worker" => Some(ActionDefinition {
                name: "delete_worker".to_string(),
                description: "Delete a virtual machine".to_string(),
                parameters: vec![
                    param!("worker_name", "Name of the VM to delete", ParamType::String, required),
                ],
            }),
            "get_worker" => Some(ActionDefinition {
                name: "get_worker".to_string(),
                description: "Get information about a virtual machine".to_string(),
                parameters: vec![
                    param!("worker_name", "Name of the VM", ParamType::String, required),
                ],
            }),
            "has_worker" => Some(ActionDefinition {
                name: "has_worker".to_string(),
                description: "Check if a virtual machine exists".to_string(),
                parameters: vec![
                    param!("worker_name", "Name of the VM", ParamType::String, required),
                ],
            }),
            "start_worker" => Some(ActionDefinition {
                name: "start_worker".to_string(),
                description: "Start a virtual machine".to_string(),
                parameters: vec![
                    param!("worker_name", "Name of the VM to start", ParamType::String, required),
                ],
            }),
            "get_volumes" => Some(ActionDefinition {
                name: "get_volumes".to_string(),
                description: "List all virtual disk volumes".to_string(),
                parameters: vec![],
            }),
            "has_volume" => Some(ActionDefinition {
                name: "has_volume".to_string(),
                description: "Check if a disk volume exists".to_string(),
                parameters: vec![
                    param!("disk_path", "Path to the disk", ParamType::String, required),
                ],
            }),
            "create_volume" => Some(ActionDefinition {
                name: "create_volume".to_string(),
                description: "Create a new disk volume".to_string(),
                parameters: vec![
                    param!("disk_path", "Path for the new disk", ParamType::String, required),
                    param!("size_mb", "Size in MB", ParamType::Integer, required),
                ],
            }),
            "delete_volume" => Some(ActionDefinition {
                name: "delete_volume".to_string(),
                description: "Delete a disk volume".to_string(),
                parameters: vec![
                    param!("disk_path", "Path to the disk", ParamType::String, required),
                ],
            }),
            "attach_volume" => Some(ActionDefinition {
                name: "attach_volume".to_string(),
                description: "Attach a disk to a VM".to_string(),
                parameters: vec![
                    param!("worker_name", "Name of the VM", ParamType::String, required),
                    param!("controller_type", "Type of controller (IDE, SCSI, DVD)", ParamType::String, optional, json!("SCSI")),
                    param!("disk_path", "Path to the disk", ParamType::String, required),
                ],
            }),
            "detach_volume" => Some(ActionDefinition {
                name: "detach_volume".to_string(),
                description: "Detach a disk from a VM".to_string(),
                parameters: vec![
                    param!("worker_name", "Name of the VM", ParamType::String, required),
                    param!("controller_type", "Type of controller (IDE, SCSI, DVD)", ParamType::String, optional, json!("SCSI")),
                    param!("disk_path", "Path to the disk", ParamType::String, required),
                ],
            }),
            "create_snapshot" => Some(ActionDefinition {
                name: "create_snapshot".to_string(),
                description: "Create a snapshot of a VM".to_string(),
                parameters: vec![
                    param!("worker_name", "Name of the VM", ParamType::String, required),
                    param!("snapshot_name", "Name of the snapshot", ParamType::String, required),
                ],
            }),
            "delete_snapshot" => Some(ActionDefinition {
                name: "delete_snapshot".to_string(),
                description: "Delete a snapshot of a VM".to_string(),
                parameters: vec![
                    param!("worker_name", "Name of the VM", ParamType::String, required),
                    param!("snapshot_name", "Name of the snapshot", ParamType::String, required),
                ],
            }),
            "has_snapshot" => Some(ActionDefinition {
                name: "has_snapshot".to_string(),
                description: "Check if a snapshot exists".to_string(),
                parameters: vec![
                    param!("worker_name", "Name of the VM", ParamType::String, required),
                    param!("snapshot_name", "Name of the snapshot", ParamType::String, required),
                ],
            }),
            "reboot_worker" => Some(ActionDefinition {
                name: "reboot_worker".to_string(),
                description: "Reboot a VM".to_string(),
                parameters: vec![
                    param!("worker_name", "Name of the VM", ParamType::String, required),
                ],
            }),
            "configure_networks" => Some(ActionDefinition {
                name: "configure_networks".to_string(),
                description: "Configure network settings for a VM".to_string(),
                parameters: vec![
                    param!("worker_name", "Name of the VM", ParamType::String, required),
                    param!("switch_name", "Name of the virtual switch", ParamType::String, required),
                ],
            }),
            "set_worker_metadata" => Some(ActionDefinition {
                name: "set_worker_metadata".to_string(),
                description: "Set metadata for a VM".to_string(),
                parameters: vec![
                    param!("worker_name", "Name of the VM", ParamType::String, required),
                    param!("key", "Metadata key", ParamType::String, required),
                    param!("value", "Metadata value", ParamType::String, required),
                ],
            }),
            "snapshot_volume" => Some(ActionDefinition {
                name: "snapshot_volume".to_string(),
                description: "Clone a disk volume".to_string(),
                parameters: vec![
                    param!("source_volume_path", "Path to the source disk", ParamType::String, required),
                    param!("target_volume_path", "Path for the cloned disk", ParamType::String, required),
                ],
            }),
            _ => None,
        }
    }
    
    fn execute_action(&self, action: &str, params: &HashMap<String, Value>) -> ActionResult {
        match action {
            "test_install" => self.test_install(),
            "list_workers" => self.list_workers(),
            "create_worker" => {
                let worker_name = validation::extract_string(params, "worker_name")?;
                let memory_mb = validation::extract_int_opt(params, "memory_mb")?.unwrap_or(2048);
                let cpu_count = validation::extract_int_opt(params, "cpu_count")?.unwrap_or(2);
                let generation = validation::extract_int_opt(params, "generation")?.unwrap_or(2);
                let switch_name = validation::extract_string_opt(params, "switch_name")?.unwrap_or_else(|| "Default Switch".to_string());
                
                self.create_worker(worker_name, memory_mb, cpu_count, generation, switch_name)
            },
            "delete_worker" => {
                let worker_name = validation::extract_string(params, "worker_name")?;
                self.delete_worker(worker_name)
            },
            "get_worker" => {
                let worker_name = validation::extract_string(params, "worker_name")?;
                self.get_worker(worker_name)
            },
            "has_worker" => {
                let worker_name = validation::extract_string(params, "worker_name")?;
                self.has_worker(worker_name)
            },
            "start_worker" => {
                let worker_name = validation::extract_string(params, "worker_name")?;
                self.start_worker(worker_name)
            },
            "get_volumes" => self.get_volumes(),
            "has_volume" => {
                let disk_path = validation::extract_string(params, "disk_path")?;
                self.has_volume(disk_path)
            },
            "create_volume" => {
                let disk_path = validation::extract_string(params, "disk_path")?;
                let size_mb = validation::extract_int(params, "size_mb")?;
                self.create_volume(disk_path, size_mb)
            },
            "delete_volume" => {
                let disk_path = validation::extract_string(params, "disk_path")?;
                self.delete_volume(disk_path)
            },
            "attach_volume" => {
                let worker_name = validation::extract_string(params, "worker_name")?;
                let controller_type = validation::extract_string_opt(params, "controller_type")?.unwrap_or_else(|| "SCSI".to_string());
                let disk_path = validation::extract_string(params, "disk_path")?;
                
                self.attach_volume(worker_name, controller_type, disk_path)
            },
            "detach_volume" => {
                let worker_name = validation::extract_string(params, "worker_name")?;
                let controller_type = validation::extract_string_opt(params, "controller_type")?.unwrap_or_else(|| "SCSI".to_string());
                let disk_path = validation::extract_string(params, "disk_path")?;
                
                self.detach_volume(worker_name, controller_type, disk_path)
            },
            "create_snapshot" => {
                let worker_name = validation::extract_string(params, "worker_name")?;
                let snapshot_name = validation::extract_string(params, "snapshot_name")?;
                self.create_snapshot(worker_name, snapshot_name)
            },
            "delete_snapshot" => {
                let worker_name = validation::extract_string(params, "worker_name")?;
                let snapshot_name = validation::extract_string(params, "snapshot_name")?;
                self.delete_snapshot(worker_name, snapshot_name)
            },
            "has_snapshot" => {
                let worker_name = validation::extract_string(params, "worker_name")?;
                let snapshot_name = validation::extract_string(params, "snapshot_name")?;
                self.has_snapshot(worker_name, snapshot_name)
            },
            "reboot_worker" => {
                let worker_name = validation::extract_string(params, "worker_name")?;
                self.reboot_worker(worker_name)
            },
            "configure_networks" => {
                let worker_name = validation::extract_string(params, "worker_name")?;
                let switch_name = validation::extract_string(params, "switch_name")?;
                
                self.configure_networks(worker_name, switch_name)
            },
            "set_worker_metadata" => {
                let worker_name = validation::extract_string(params, "worker_name")?;
                let key = validation::extract_string(params, "key")?;
                let value = validation::extract_string(params, "value")?;
                
                self.set_worker_metadata(worker_name, key, value)
            },
            "snapshot_volume" => {
                let source_volume_path = validation::extract_string(params, "source_volume_path")?;
                let target_volume_path = validation::extract_string(params, "target_volume_path")?;
                
                self.snapshot_volume(source_volume_path, target_volume_path)
            },
            _ => Err(format!("Action '{}' not found", action)),
        }
    }
}