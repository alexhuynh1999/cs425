import torch
import sys

def main():
    model = torch.hub.load('pytorch/vision:v0.10.0', 'alexnet', pretrained=True)
    print(sys.argv)
    torch.save(model, f"./models/model_{sys.argv[1]}.pt")

if __name__ == "__main__":
    main()