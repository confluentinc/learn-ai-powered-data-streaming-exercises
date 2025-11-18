# learn-ai-powered-data-streaming-exercises

This repository is part of the Confluent organization on GitHub.
It is public and open to contributions from the community.

Please see the LICENSE file for contribution terms.
Please see the CHANGELOG.md for details of recent updates.

# Workshop Introduction

In this workshop, you will be building a real-time weather analytics pipeline using Confluent Cloud, Apache Kafka, Apache Flink, and Hugging Face.

Instructions for the exercises can be found at [developer.confluent.io/courses/ai-powered-data-streaming](https://developer.confluent.io/courses/ai-powered-data-streaming)

## Exercise Workflow

All of your work will be done inside the `./exercises` folder.

Inside the folder you will find a script named `./exercise.sh`. This script contains useful commands to help manage the exercise workflow.

### Available Commands

#### List All Exercises
```bash
./exercise.sh list
```
Shows all available exercises from the solutions directory.

#### Stage an Exercise
```bash
./exercise.sh stage <exercise-filter>
```

Copies starter files (such as tests) from the staging directory to your working directory.

**Example:**
```bash
./exercise.sh stage 01
```

**What it does:**
- Copies the starter template for the specified exercise into your `exercises/` directory
- Sets up the basic project structure with placeholder files or tests
- You'll work on these files to complete the exercise

#### Solve an Exercise (Get the Solution)
```bash
./exercise.sh solve <exercise-filter>
```
Copies the complete solution files for the specified exercise.

**Example:**
```bash
./exercise.sh solve 01
```

**What it does:**
- Copies all solution files from the specified exercise
- Overwrites your working files with the complete solution
- Useful if you get stuck and want to replace your solution with the official solution.

#### Solve a Specific File
```bash
./exercise.sh solve <exercise-filter> <file-filter>
```
Copies just one specific file from the solution instead of everything.

**Example:**
```bash
# Copy just the main.py file from exercise 01
./exercise.sh solve 01 main.py
```

**What it does:**
- Finds and copies only the matching file from the solution
- Leaves all other files untouched
- Useful when you are stuck on just one component and need the solution, but don't want to replace all of your code.

### Typical Exercise Workflow

1. **Start a new exercise:**
   ```bash
   ./exercise.sh stage 01
   ```

2. **Work on the exercise** by editing the files in the `./exercises` folder.

3. **If you get stuck on a specific file:**
   ```bash
   ./exercise.sh solve 01 main.py
   ```

4. **Or get the complete solution:**
   ```bash
   ./exercise.sh solve 01
   ```

## Project Structure

```
learn-ai-powered-data-streaming-exercises/
├── README.md                        # This file - workshop overview
├── CHANGELOG.md                     # Version history and updates
├── LICENSE                          # License and contribution terms
├── service.yml                      # Service configuration
│
├── exercises/                       # Your working directory
│   └── exercise.sh                  # Helper script for managing exercises
│
├── staging/                         # Starter templates for each exercise
│   └── (exercise templates copied by `exercise.sh stage`)
│
└── solutions/                       # Complete solutions for each exercise
    └── (solution files copied by `exercise.sh solve`)
```