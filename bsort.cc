#include <cstdlib>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <chrono>

#include "library.h"
#include "leveldb/db.h"
#include "json/json.h"


using namespace std;

int main(int argc, const char* argv[]) {

	if (argc < 5) {
		cout << "ERROR: invalid input parameters!" << endl;
		cout << "Please enter <schema_file> <input_file> <out_index> <sorting_attributes>" << endl;
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

	FILE* in_fp = fopen (argv[2] , "r");
  	FILE* out_fp = fopen(argv[3], "r+");
	

	// Initiating the Schema object
	Schema *cur_schema =  new Schema();
	cur_schema -> n_sort_attrs = argc - 4;
	cur_schema -> nattrs = schema.size();

	int cur_offset = 0;
	for (int i = 0; i < schema.size(); i++) {

		cur_schema -> attrs[i] = new Attribute();
		strcpy(cur_schema -> attrs[i] -> name, schema[i].get("name", "UTF-8" ).asString().c_str());
		cur_schema -> attrs[i] -> length = schema[i].get("length", "UTF-8").asInt();
		cur_schema -> offset[i] = cur_offset;
		cur_offset = cur_offset + cur_schema -> attrs[i] -> length + 1;
		strcpy(cur_schema -> attrs[i] -> type, schema[i].get("type", "UTF-8" ).asString().c_str());
		
		string attr_type = schema[i].get("type", "UTF-8").asString();
		if(attr_type != "string") {
			strcpy(cur_schema -> attrs[i] -> distribution, schema[i].get("distribution", "UTF-8" ).get("name", "UTF-8").asString().c_str());
		}
		
	}
	int bytes_per_record = cur_schema -> bytes_per_record();
	
	for(int i = 0; i < cur_schema -> n_sort_attrs; i++) {
		string cur_att(argv[i + 4]);
		if(cur_att == "student_number") cur_schema -> sort_attrs[i] = 0;
		else if(cur_att == "account_name") cur_schema -> sort_attrs[i] = 1;
		else if(cur_att == "start_year") cur_schema -> sort_attrs[i] = 2;
		else cur_schema -> sort_attrs[i] = 3;
	}

	// Do work here
	leveldb::DB *db;
	leveldb::Options options;
	options.create_if_missing = true;
	leveldb::Status status = leveldb::DB::Open(options, "./leveldb_dir", &db);
	assert(status.ok());

	
	char buffer[50];
	char str[100];
	char unique_key[10 + sizeof(long)]; 
	long unique_counter = 0;

	auto start = chrono::steady_clock::now();
	while ( !feof (in_fp) ) {
		fgets (buffer, 1 , in_fp);

		//int bytes_per_record = cur_schema -> bytes_per_record() + 1;
		if ( fgets (buffer, bytes_per_record + 1, in_fp) == NULL ) break;
		
		unique_counter++;
		int length = 0;
		
		for(int j = 0; j < cur_schema -> nattrs; j++) {
			int i = cur_schema -> sort_attrs[j];
			int offset = cur_schema -> offset[i];
			memcpy(str + length, buffer + offset, cur_schema -> attrs[i] -> length);
			length += cur_schema -> attrs[i] -> length;
		}

		memcpy(str + length, &unique_counter, sizeof(long));
		str[length + sizeof(long)] = '\0';
            
		db->Put(leveldb::WriteOptions(), leveldb::Slice(str, sizeof(str)), buffer);
		fgets (buffer, 2 , in_fp); //skips end of line character	
	}

	int cnt = 0;
	leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
	for (it->SeekToFirst(); it->Valid(); it->Next()) {
		leveldb::Slice key = it->key();
		leveldb::Slice value = it->value();
		std::string key_str = key.ToString();
		std::string val_str = value.ToString();
		cnt++;
		strcpy(buffer, val_str.c_str());
		fprintf(out_fp, buffer);
        fprintf(out_fp, "\n");
	}
	assert(it->status().ok());  // Check for any errors found during the scan

	auto end = chrono::steady_clock::now();
	auto nanosec = chrono::duration_cast<chrono::nanoseconds>(end - start).count();
    double duration = nanosec*1.0/(1e6 * 1.0);
    cout << "TIME : "
        << duration
        << "milliseconds" << endl;
	
	// delete db;
	for (it->SeekToFirst(); it->Valid(); it->Next()) {
		leveldb::Slice key = it->key();
		leveldb::Slice value = it->value();
		db->Delete(leveldb::WriteOptions(), key);
	}
	assert(it->status().ok());  // Check for any errors found during the scan
	delete it;
	delete db;
	return 0;
}