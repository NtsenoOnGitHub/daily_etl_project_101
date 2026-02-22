import os

# Writes error and success messages on the program.log file
# Takes in the message to be written on the log file
# The function does not return anything.
def log(message: str):
    if os.path.exists('logs/program.log'):
        with open('logs/program.log', "a") as file:
            file.write(message)
    else:
        file = open("logs/program.log", "x")
        file.write(message)