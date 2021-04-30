#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <fstream>

#include "library.h"
#include "json/json.h"

using namespace std;

int main(int argc, const char* argv[]) {
  if (argc < 7) {
    cout << "ERROR: invalid input parameters!" << endl;
    cout << "Please enter <schema_file> <input_file> <output_file> <mem_capacity> <k> <sorting_attributes>" << endl;
    exit(1);
  }
  string schema_file(argv[1]);

  // Parse the schema JSON file
  Json::Value schema;
  Json::Reader json_reader;
  // Support for std::string argument is added in C++11
  // so you don't have to use .c_str() if you are on that.
  ifstream schema_file_istream(schema_file.c_str(), ifstream::binary);
  bool successful = json_reader.parse(schema_file_istream, schema, false);
  if (!successful) {
    cout << "ERROR: " << json_reader.getFormatedErrorMessages() << endl;
    exit(1);
  }

  // Print out the schema
  string attr_name;
  int attr_len;
  for (int i = 0; i < schema.size(); ++i) {
    attr_name = schema[i].get("name", "UTF-8" ).asString();
    attr_len = schema[i].get("length", "UTF-8").asInt();
    cout << "{name : " << attr_name << ", length : " << attr_len << "}" << endl;
  }

  // Initiating the Schema object
  Schema *cur_schema =  new Schema();
  cur_schema -> n_sort_attrs = argc - 6;
  cur_schema -> nattrs = schema.size();
  for (int i = 0; i < schema.size(); i++) {

    cur_schema -> attrs[i] = new Attribute();
    strcpy(cur_schema -> attrs[i] -> name, schema[i].get("name", "UTF-8" ).asString().c_str());
    cur_schema -> attrs[i] -> length = schema[i].get("length", "UTF-8").asInt();
    strcpy(cur_schema -> attrs[i] -> type, schema[i].get("type", "UTF-8" ).asString().c_str());
    
    string attr_type = schema[i].get("type", "UTF-8").asString();
    if(attr_type != "string") {
      strcpy(cur_schema -> attrs[i] -> distribution, schema[i].get("distribution", "UTF-8" ).get("name", "UTF-8").asString().c_str());
    }
    
  }
  
  for(int i = 0; i < cur_schema -> n_sort_attrs; i++) {
    string cur_att(argv[i + 6]);
    if(cur_att == "student_number") cur_schema -> sort_attrs[i] = 1;
    else if(cur_att == "account_name") cur_schema -> sort_attrs[i] = 2;
    else if(cur_att == "start_year") cur_schema -> sort_attrs[i] = 3;
    else cur_schema -> sort_attrs[i] = 4;
  }

  // Do the sort
  FILE* in_fp = fopen (argv[2] , "r");;
  FILE* out_fp = fopen(argv[3], "w");
  mk_runs(in_fp, out_fp, 10, cur_schema);

  // in_fp = fopen (argv[2] , "r");
  // while ( ! feof (in_fp) )
  //    {
  //      if ( fgets (buffer , 100 , in_fp) == NULL ) break;
  //      fputs (buffer , stdout);
  //    }
  //    fclose (in_fp);
  
  // return 0;
}
