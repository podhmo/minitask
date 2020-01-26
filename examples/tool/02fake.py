from minitask.port import fake


output_port = fake.create_writer_port()
input_port = fake.create_reader_port()

fake.send("hello", file=output_port)
msg = fake.recv(file=input_port)
print(msg)
