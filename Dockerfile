# Use an official Python runtime as a parent image
FROM python:3.6

#ENV http_proxy http://proxy-chain.xxx.com:911/ 
#ENV https_proxy http://proxy-chain.xxx.com:912/ 

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Make port 13800 available to the world outside this container
EXPOSE 13800

# Run server.py when the container launches
CMD ["python", "server.py"]
