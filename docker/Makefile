
CONTAINER_NAME=jupyter_spark
PEM_F=/Users/wmcabee/_SPARK/data/sparkkey.pem

.PHONY: jupyter
jupyter:
	docker run --rm -p 8888:8888 -p 4040:4040 -p 4041:4041 \
		--name $(CONTAINER_NAME) \
		-e JUPYTER_ENABLE_LAB=yes \
		-v "$(PWD)/..":/home/jovyan/work \
		-v /Users/wmcabee/_SPARK/data:/home/jovyan/data \
		jupyter/pyspark-notebook:1386e2046833

.PHONY: ssh
bash: 
	docker exec -it $(CONTAINER_NAME) /bin/bash

.PHONY: master
master: 
	ssh -i $PEM_F hadoop@ec2-3-216-126-200.compute-1.amazonaws.com

