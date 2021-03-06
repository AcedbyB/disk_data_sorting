'''
You should implement this script to generate test data for your
merge sort program.

The schema definition should be separate from the data generation
code. See example schema file `schema_example.json`.
'''
import random
import string


def generate_data(schema, out_file, nrecords):
  '''
  Generate data according to the `schema` given,
  and write to `out_file`.
  `schema` is an list of dictionaries in the form of:
    [ 
      {
        'name' : <attribute_name>, 
        'length' : <fixed_length>,
        ...
      },
      ...
    ]
  `out_file` is the name of the output file.
  The output file must be in csv format, with a new line
  character at the end of every record.
  '''
  f = open(output, "a")

  for i in range(0, nrecords):
    for field in schema:
      if field["type"] == "string":
        l = field["length"]
        letters = string.ascii_lowercase
        str_res = ''.join(random.choice(letters) for k in range(l)) 
        f.write(str_res)
      else:
        if field["distribution"]["name"] == "uniform":
          mn = field["distribution"]["min"]
          mx = field["distribution"]["max"]
          value = random.randint(mn, mx)
          f.write(str(value))
        else:
          mu = field["distribution"]["mu"]
          sigma = field["distribution"]["sigma"]
          value = random.gauss(mu, sigma)
          f.write(format(value, ".2f"))
       
      
      f.write(",")
    f.write("\n")

  f.close()
  print("Generating %d records" % nrecords)

if __name__ == '__main__':
  import sys, json
  if not len(sys.argv) == 4:
    print("data_generator.py <schema json file> <output csv file> <# of records>")
    sys.exit(2)

  schema = json.load(open(sys.argv[1]))
  output = sys.argv[2]
  nrecords = int(sys.argv[3])
  print(schema)
  
  generate_data(schema, output, nrecords)

