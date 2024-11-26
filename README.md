# Ray Demo

This is a simple Ray workflow approximating pi.
Given the `Imgagespec` + `requirements.txt`, this is the minimal set of dependencies to run Ray in Union and have all the functionalities in the Ray dashboard enabled (except Grafana Dashboards).

## Run the Demo
1. `pip install -r requirements.txt`
2. `union run --remote approximate_pi.py wf`