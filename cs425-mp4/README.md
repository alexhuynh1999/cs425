# cs425-mp4



## Getting started

Please make sure you have all the dependencies:  
Pillow  
torch  
torchvision  
numpy  

## Initialization
Coordinator should start first. To start the Coordinator: $go run coordinator.go  

The rest of the nodes join as Worker. To start a Worker: $go run worker.go  
Then to join the coordinator, type: $join  
Message will appear to indicate a succesfful join. Else, terminate and rerun worker.go  

## Commands
To train a model with batch size, use $train [batch size]  
To run inference after training models, use $inference [batch size]  

To view tasks allocated on each vm, use $ls-jobs  
To view query rate, use $get-rate [model number]  
To see stats, use $stats [model number]  

