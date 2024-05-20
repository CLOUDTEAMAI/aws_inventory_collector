
def hello_haim(input):
    print(input)


a = globals()

tasks = {
    "haim": globals()['hello_haim']
}

print(globals())
