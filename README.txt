Test idea - Run a query multiple times against url in an env with unrestricted bandwidth to stress test a datasets’ response time and speed on a particular host. The idea is to simulate real world scenarios, such as a workshop with many users trying to access a dataset.

Idea:
use EKS/ECS/Fargate 
define task to be run as a image
gather metrics from each execution (specifically network)