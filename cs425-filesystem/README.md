# CS425-MP1 /

## Installation
To install the repository, simply go into your directory and clone the repo.
```
cd [your directory]
git clone [http/ssh]
```

## Usage
### Setup
Either use the VMs fa22-cs425-84XX.cs.illinois.edu where XX is from 01 to 10, or edit `machine.go` line 34 such that the desired IP addresses/ports are there. 

On each machine, run the command:
```
go run machine.go
```

Some dependencies may need to be installed. Once the program is running, the terminal should ask for which VM is being used. Enter the 0-based index from `machine.go:30`. The output on terminal should look like:
```
[ah10@fa22-cs425-8401 cs425-filesystem]$ go run machine.go

Select VM:
```

Once the VM is selected, there should be an output that looks like
```
command: 2022/09/25 18:03:34 listening successful @ 172.22.95.22:6000

```

Type any of the supported commands, `introduce`, `join`, `membership`, `leave`, or `rejoin`.

### Introduce
In order to have any machine join the network, the introducer must join first. On the machine designed as introducer, designated on `machine.go:55`, type `introduce` as the first command. If it's successful, the output should look like:
```
introduce

command:
```

From here, the other commands can be used.

### Join
**This command can only be used once the introducer is in the network**. Also, this only works for processes **first time joining**. For leave and rejoins, see `rejoin`. 

Simply enter `join` on the terminal, and the machine will join the network. Machines on the network have access to the other commands, like `membership` or `leave`.

### Membership
This can only be done on machines currently in the network. Running without on the network will throw a warning.

Type `membership` into the terminal to see the history of members in the network, dead and alive.

### Leave and Rejoin
This can only be done on machines currently in the network. Running without on the network will throw a warning.

Type `leave` to exit to the command-line interface. To rejoin the network, type `rejoin` instead of `join`.
