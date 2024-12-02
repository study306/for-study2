import streamlit as st

# Define the experiments with code, file names, and commands
experiments = {
    "Hadoop command lines": {
        "filename": "hadoop_operations.sh",
        "code": """
# Start Hadoop Node
sudo /home/cloudera/cloudera-manager --express --force

# Verify the Working Environment
pwd
whoami

# Create a new directory in Linux
mkdir newdirectory
cd newdirectory
touch newfile.txt
vi newfile.txt
cat newfile.txt

# Perform Operations in HDFS
hdfs dfs -ls /
hdfs dfs -mkdir /user/cloudera/newdirectory
hdfs dfs -put newfile.txt /user/cloudera/newdirectory
hdfs dfs -cat /user/cloudera/newdirectory/newfile.txt
hdfs dfs -get /user/cloudera/newdirectory/newfile.txt /home/cloudera/newdirectory

# Install the MovieLens Dataset
wget http://media.sundog-soft.com/hadoop/ml-100k/u.data
hdfs dfs -mkdir /user/cloudera/movielens
hdfs dfs -put u.data /user/cloudera/movielens
hdfs dfs -ls /user/cloudera/movielens
hdfs dfs -cat /user/cloudera/movielens/u.data | head -10
        """,
        "execution_command": "sudo /home/cloudera/cloudera-manager --express --force; pwd; whoami; hdfs dfs -ls /; hdfs dfs -mkdir /user/cloudera/newdirectory; hdfs dfs -put newfile.txt /user/cloudera/newdirectory; hdfs dfs -cat /user/cloudera/newdirectory/newfile.txt; hdfs dfs -get /user/cloudera/newdirectory/newfile.txt /home/cloudera/newdirectory; wget http://media.sundog-soft.com/hadoop/ml-100k/u.data; hdfs dfs -mkdir /user/cloudera/movielens; hdfs dfs -put u.data /user/cloudera/movielens; hdfs dfs -ls /user/cloudera/movielens; hdfs dfs -cat /user/cloudera/movielens/u.data | head -10",
        "sample_file": "newfile.txt"
    },
    "MapReduce Word Count": {
        "filename": "mapper_reducer_wordcount.sh",
        "code": """
# Mapper Code (mapper.py)
#!/usr/bin/python3
import sys
for line in sys.stdin:
    line = line.strip()
    words = line.split()
    for word in words:
        print('%s\\t%s' % (word, 1))

# Reducer Code (reducer.py)
#!/usr/bin/python3
import sys
current_word = None
current_count = 0
for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\\t', 1)
    try:
        count = int(count)
    except ValueError:
        continue
    if current_word == word:
        current_count += count
    else:
        if current_word:
            print('%s\\t%s' % (current_word, current_count))
        current_word = word
        current_count = count
if current_word == word:
    print('%s\\t%s' % (current_word, current_count))

# Execution Command:
cat wordfile.txt | python3 mapper.py | sort -k1,1 | python3 reducer.py

# file creation: 
echo -e "hello world\nhello from the other side\nworld is beautiful" > wordfile.txt

# Run the Mapper:
cat wordfile.txt | python3 mapper.py

# Sort the Mapper Output
cat wordfile.txt | python3 mapper.py | sort -k1,1 > sorted_output.txt

# Run the Reducer:
cat sorted_output.txt | python3 reducer.py
        """,
        "execution_command": "cat wordfile.txt | python3 mapper.py | sort -k1,1 | python3 reducer.py",
        "sample_file": "wordfile.txt"
    },
    "MapReduce Rank Movies by Popularity": {
        "filename": "mapper_reducer_rank_movies.sh",
        "code": """
# Mapper Code (mapper.py)
#!/usr/bin/python3
import sys
for line in sys.stdin:
    line = line.strip()
    parts = line.split(',')
    if len(parts) == 3:
        movie_id, movie_name, popularity_score = parts
        try:
            popularity_score = float(popularity_score)
            print('%s\t%s\t%f' % (movie_name, movie_id, popularity_score))
        except ValueError:
            continue

# Reducer Code (reducer.py)
#!/usr/bin/python3
import sys
movies = []

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\\t')
    if len(parts) == 3:
        movie_name, movie_id, popularity_score = parts
        try:
            popularity_score = float(popularity_score)
            movies.append((popularity_score, movie_name, movie_id))
        except ValueError:
            continue

movies.sort(reverse=True, key=lambda x: x[0])

for movie in movies:
    print('%s\\t%s\\t%f' % (movie[1], movie[2], movie[0]))
    
    
# file creation: 
echo -e "1,The Matrix,8.7\n2,Inception,8.8\n3,The Dark Knight,9.0\n4,Interstellar,8.6\n5,The Prestige,8.5" > movies.txt

# Run the Mapper:
cat movies.txt | python3 mapper.py

# Sort the Mapper Output
cat movies.txt | python3 mapper.py | sort -k3,3nr > sorted_movies.txt

# Run the Reducer:
cat sorted_movies.txt | python3 reducer.py

# Execution Command:
cat movies.txt | python3 mapper.py | sort -k3,3nr | python3 reducer.py
        """,
        "execution_command": "cat movies.txt | python3 mapper.py | sort -k3,3nr | python3 reducer.py",
        "sample_file": "movies.txt"
    }
}

# Create Streamlit App
st.set_page_config(page_title="MapReduce Experiments", layout="wide")

# Sidebar for selecting experiments
st.sidebar.title("Experiments")
selected_experiment = st.sidebar.selectbox("Choose an Experiment", list(experiments.keys()))

# Show the selected experiment details
experiment = experiments[selected_experiment]
st.title(selected_experiment)

# Display experiment code
st.subheader("Code")
st.code(experiment["code"], language="bash")

# Display execution command
st.subheader("Execution Command")
st.code(experiment["execution_command"], language="bash")

# Provide a download button for the script file
st.subheader(f"Download {experiment['filename']}")
st.download_button(
    label=f"Download {experiment['filename']}",
    data=experiment["code"],
    file_name=experiment["filename"],
    mime="application/bash"
)

# If there's a sample file, provide download for it
if experiment["sample_file"]:
    st.subheader(f"Sample File: {experiment['sample_file']}")
    with open(experiment["sample_file"], "w") as f:
        if experiment["sample_file"] == "wordfile.txt":
            f.write("Hello world this is a test\nAnother line with words\n")
        elif experiment["sample_file"] == "movies.txt":
            f.write("1,Movie A,100\n2,Movie B,90\n3,Movie C,110\n")
        elif experiment["sample_file"] == "newfile.txt":
            f.write("Hello, this is a sample text file for Hadoop operations.\n")
    
    st.download_button(
        label=f"Download {experiment['sample_file']}",
        data=open(experiment["sample_file"], "rb").read(),
        file_name=experiment["sample_file"],
        mime="application/octet-stream"
    )
