# **Object Engine (H1)**

Basic Flow:
The Engine uses SFTP to get inputs . 
1. The folder object_engine will get new sub folder under it for any new query.
2. The sub-folder name shall be in nomenclature <18 alphanumeric characters><ddmmyyyyhhmmss>.
3. Any inelegible folder as per naming comvention will be moved to object_engine_reject.
4. If any folder doesn't have valid objects.json and rules.json, they shall be moved to object_engine_failed.
5. Successfully processed folder shall be moved to object_engine_processed along with a results.json.

   
Components:

==SFTP
1. It serves as the central storage location fo the data.
2. It needs to have 4 folders i.e 
	- object_engine(Input Folder)
	- object_engine_processed(Results Folder)
	- object_engine_reject(folders not eligible for processing)
	-	object_engine_failed(Folders failed to process)

==Redis
1. Used by Core Engine for counter and lock mechanism.

==Core Engine
1. This component listens on port 5000 for POSt method accepting a folder name argument.
2. It validates the folder on SFTP for existence and fulfillement of multiple criterias and take take actions accordingly.
3. It uses Redis for counters and locking mechanism at different stages.
4. Finally , it return a response and also uplods to the SFTP.
5. Appropriate retry mechanism have been configured to ensure efficiency.
6. It can have multiple replicas running behind a load balancer to ensure scalability and reliability.

==Monitor
1. This component runs on recurrning basis(frequency can be controlled) and monitors the SFTP for input data.
2. Based on eligible inputs, it calls the Core Engine for processing.
3. Appropriate retry mechanism have been configured to ensure efficiency and reliability.
4. Ina production environment, recommended to be hosted as scheduled joblike K8 Job or AWS Lambda Function.
