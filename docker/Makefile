
.PHONY: run
run:
	docker run --rm -p 8888:8888 -e JUPYTER_ENABLE_LAB=yes -v "$(PWD)/..":/home/jovyan/work jupyter/pyspark-notebook:1386e2046833
	#docker run --rm -p 8889:8888 -e JUPYTER_ENABLE_LAB=yes jupyter/pyspark-notebook:1386e2046833
	#docker run --rm -p 8888:8888 jupyter/pyspark-notebook:latest
