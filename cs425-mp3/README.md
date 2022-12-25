# CS425-MP3 /

## Installation
To install the repository, simply go into your directory and clone the repo.
```
cd [your directory]
git clone [http/ssh]
```

## Usage
### Setup
Either use the VMs fa22-cs425-84XX.cs.illinois.edu where XX is from 01 to 10, or edit `data_node.go` such that the desired IP addresses/ports are there. 

Navigate to the root directory, and on any VM 1-5, run the following command:
```
go run leader.go
```

Some dependencies may need to be installed. Once the program is running, any machine should be able to run.

On every other VM, run 
```
go run data_node.go
```

For full command functionality on the active leader, go back and close the VM that ran `go run leader.go` and run 
```
go run data_node.go
```

### Join
**This command can only be used once the active leader is in the network**

On any `data_node`, type `join` to join the network. 

### Membership
This can only be done on machines currently in the network. Running without on the network will throw a warning.

Type `list_mem` into the terminal to see current members of the network.

Type `list_self` to identify which machine this is.

### Leave
To end the process / kill this `data_node`, simply type `leave` into the terminal. Alternatively, Ctrl+C should work on Windows

### Put
Purpose: Place a local file into a custom SDFS file name.

Template usage:
```
query put [local_file_name] [sdfs_file_name] [version]
```

Note that all file commands will need to be followed by the 'query' keyword.

### Get
Purpose: Retrieve an SDFS file and store it in the local directory. To get all versions, use `get-versions`

Template usage:
```
query get [sdfs_file_name] [version] [local_file_name]
```

Template usage:
```
query get-versions [sdfs_file_name] [# versions] [local_file_name]
```

### Delete
Purpose: Remove an SDFS file from the network.

Template usage:
```
query delete [sdfs_file_name] [version]
```
Note that `version` is optional. If version is not specified, all versions of the file will be removed.

### Lookup
Purpose: Find where an SDFS file is stored or how many versions of the file exist in the network.

Template usage:
```
query lsAdr [sdfs_file_name] [version]
```

Template usage:
```
query lsVer [sdfs_file_name]
```

### Store
Purpose: See what SDFS files are stored at this location.

Template usage:
```
store
```