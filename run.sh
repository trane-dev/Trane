python3 generate_tasks.py
cat tasks.json | python3 -m json.tool > tasks_pretty.json
python3 generate_labels.py
