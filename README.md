fka Go Worker

## Description
This project is a Kafka consumer written in Go that listens for messages on a specified topic ("comments") and processes them. It handles graceful shutdowns and logs the received messages along with their count.

## Installation
1. Ensure you have Go installed on your machine. You can download it from [golang.org](https://golang.org/dl/).
2. Clone the repository:
   ```bash
      git clone <repository-url>
         cd <repository-directory>
	    ```
	    3. Install the required dependencies:
	       ```bash
	          go mod tidy
		     ```

		     ## Usage
		     1. Start your Kafka broker and ensure it is running.
		     2. Run the worker:
		        ```bash
			   go run worker/worker.go
			      ```
			      3. The worker will start consuming messages from the "comments" topic. It will log the received messages and their count.

			      ## License
			      This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

