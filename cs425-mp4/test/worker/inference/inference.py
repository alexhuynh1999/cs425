import torch
import sys
from PIL import Image
from torchvision import transforms

def main():
    job_id = sys.argv[1]
    model = torch.load(f'./models/model_{job_id}.pt')
    input_image = Image.open("sdfs_query")
    preprocess = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])
    input_tensor = preprocess(input_image)
    input_batch = input_tensor.unsqueeze(0)
    with torch.no_grad():
        output = model(input_batch)
    probabilities = torch.nn.functional.softmax(output[0], dim=0)
    with open("imagenet_classes.txt", "r") as f:
        categories = [s.strip() for s in f.readlines()]
    top1_prob, top1_catid = torch.topk(probabilities, 1)
    for i in range(top1_prob.size(0)):
        print(categories[top1_catid[i]], top1_prob[i].item())

if __name__ == "__main__":
    main()