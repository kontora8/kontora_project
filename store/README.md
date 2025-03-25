# Store
## Instructions for Starting the Project
To start the Store, follow these steps:
1. Clone the repository to your local machine:
```bash
https://github.com/kontora8/kontora_project
cd store
```
2. Create and activate a virtual environment (optional but recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate
```
3. Install the project dependencies:
```bash
pip install -r requirements.txt
```
4. Run the system:
```bash
cd docker
docker-compose up --build
```
## Common Commands
### 1. Saving Requirements
To save the project dependencies to the requirements.txt file:
```bash
pip freeze > requirements.txt
```