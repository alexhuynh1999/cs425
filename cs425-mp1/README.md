# CS425-MP1 /

## Installation
To install the repository, simply go into your directory and clone the repo.
```
cd [your directory]
git clone [http/ssh]
```

## Usage
### Setup
Either use the VMs fa22-cs425-84XX.cs.illinois.edu where XX is from 01 to 10, or edit `machine.go` line 30 such that the desired IP addresses/ports are there. 

On each machine, run the command:
```
go run machine.go
```

Some dependencies may need to be installed. Once the program is running, the terminal should say if the connection / dial was successful or not. One sign of a successful program running is if the terminal looks something like
```
[ah10@fa22-cs425-8401 cs425-mp1]$ go run machine.go
2022/09/11 00:06:48 Dial success to 172.22.95.25:6000
2022/09/11 00:06:48 listening successful @ [::]:6000
2022/09/11 00:06:48 Dial success to 172.22.95.22:6000
2022/09/11 00:06:48 Dial success to 172.22.157.23:6000
2022/09/11 00:06:48 Dial success to 172.22.159.23:6000
2022/09/11 00:06:48 Dial success to 172.22.95.23:6000
2022/09/11 00:06:48 Dial success to 172.22.157.24:6000
2022/09/11 00:06:48 Dial success to 172.22.159.24:6000
2022/09/11 00:06:48 Dial success to 172.22.95.24:6000
2022/09/11 00:06:48 Dial success to 172.22.157.25:6000
2022/09/11 00:06:48 Dial success to 172.22.159.25:6000

grep
```

From here, custom `grep` commands can be written into the terminal. 

### Grep querying
**Do not type grep in to the terminal**. This is already assumed and is pre-written, even if `grep` does not appear. Here's an example command and output:
```
grep -c hello sample.txt
2022/09/11 00:09:06 grep success from: 172.22.95.22:6000
2022/09/11 00:09:06 grep success from: 172.22.157.23:6000
2022/09/11 00:09:06 grep success from: 172.22.95.23:6000
2022/09/11 00:09:06 grep success from: 172.22.159.23:6000
2
from vm1.log@172.22.95.22:6000 delay in milisec: 2
2
from vm2.log@172.22.157.23:6000 delay in milisec: 2
2
from vm4.log@172.22.95.23:6000 delay in milisec: 2
2
from vm3.log@172.22.159.23:6000 delay in milisec: 2
Total line count: 8
```

The general format for grep commands is as follows: `grep [flags] [pattern] [filename]`. Note that if `[filename]` does not exist on the VM, it will not return any matches. This is a feature, not a bug.

Also, for more robust regular expression (regex) patterns, start the pattern with the " character. Here's an example:
```
grep -c "[a-z] [0-9] sample.txt
2022/09/11 00:11:29 grep success from: 172.22.95.22:6000
2022/09/11 00:11:29 grep success from: 172.22.159.23:6000
2022/09/11 00:11:29 grep success from: 172.22.95.23:6000
2022/09/11 00:11:29 grep success from: 172.22.157.23:6000
3
from vm1.log@172.22.95.22:6000 delay in milisec: 4
3
from vm3.log@172.22.159.23:6000 delay in milisec: 4
3
from vm4.log@172.22.95.23:6000 delay in milisec: 4
3
from vm2.log@172.22.157.23:6000 delay in milisec: 4
Total line count: 12
```

### Testing
Exit to the terminal and run 
```
go test
```
