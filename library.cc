#include "library.h"
#include <iostream>
#include <string.h>
#include <stdlib.h>  
#include<vector> 
#include<algorithm>
#include<queue>

using namespace std;

int compare (const void * a, const void * b)
{
  Record *rA = *(Record **)a;
  Record *rB = *(Record **)b;
  for(int j = 0; j < rA -> schema -> n_sort_attrs; j++) {
    int i = rA -> schema -> sort_attrs[j];
    int length = rA -> schema -> attrs[i] -> length;
    int offset = rA -> schema -> offset[i];
    char str_A[length + 1];
    char str_B[length + 1];
    memcpy(str_A, rA -> data + offset, length);
    memcpy(str_B, rB -> data + offset, length);
    str_A[length] = '\0';
    str_B[length] = '\0';

    int cmp = strcmp(str_A, str_B);
    if(cmp!=0) return cmp;
  }

  return 0;
}

class Compare
{
public:
    bool operator() (RunIterator* a, RunIterator* b)
    {
        Record* rA = a->cur_record;
        Record* rB = b->cur_record;
        for(int j = 0; j < rA -> schema -> n_sort_attrs; j++) {
            int i = rA -> schema -> sort_attrs[j];
            int length = rA -> schema -> attrs[i] -> length;
            int offset = rA -> schema -> offset[i];
            char str_A[length + 1];
            char str_B[length + 1];
            memcpy(str_A, rA -> data + offset, length);
            memcpy(str_B, rB -> data + offset, length);
            str_A[length] = '\0';
            str_B[length] = '\0';

            int cmp = strcmp(str_A, str_B);
            if(cmp>0) return true;
        }
        return false;
    }
};

int Schema::bytes_per_record()
{
  int n = nattrs;
  for(int i = 0; i < nattrs; i++){
    n += attrs[i]->length;
  }
  return n;
}

void mk_runs(FILE *in_fp, FILE *out_fp, long run_length, Schema *schema)
{
  int cur = 0;
  char buffer[5];
  Record* records[run_length];
  for(int i = 0; i < run_length; i++) {
    records[i] = new Record();
    records[i] -> schema = schema;
  }

  int bytes_per_record = schema->bytes_per_record();
  while ( !feof (in_fp) ) {
    fgets (buffer, 1 , in_fp);

    if ( fgets (records[cur] -> data , schema -> bytes_per_record() + 1, in_fp) == NULL ) break;
    fgets (buffer, 2 , in_fp); //skips end of line character
    cur++;

    if(cur == run_length) {
      qsort (records, run_length, sizeof(Record*), compare);
      for(int i = 0; i < run_length; i++) {
        fprintf(out_fp, records[i] -> data);
        fprintf(out_fp, "\n");
      }
      cur = 0;
    }
  }
  fclose (in_fp);
  return;
}

/**
 * The iterator helps you scan through a run.
 * you can add additional members as your wish
 */
RunIterator::RunIterator(FILE *Fp, long Start_pos, long Run_length, long Buf_size, Schema *sChema) {
      start_pos = Start_pos;
      run_length = Run_length;
      buf_size = Buf_size;
      schema = sChema;
      fp = Fp;
      records_num = buf_size/(schema->bytes_per_record()+1);
      cur_record = new Record();
      cur_record -> schema = schema;
      record_buf = malloc(records_num*(schema->bytes_per_record()+1));
}
 
/**
 * reads the next record
 */
Record*  RunIterator::next() {
  int bytes_per_record = schema->bytes_per_record() ;
  if (cur_index % records_num == 0){
    fseek(fp, start_pos+cur_index*(bytes_per_record+1), SEEK_SET);
    fread((char*)record_buf, records_num*(bytes_per_record+1), 1, fp);
  }
  Record* record = new Record();
  record->schema = schema;
  memcpy(record->data, (char*) record_buf + (cur_index%records_num)*(bytes_per_record+1), bytes_per_record);
  cur_record = record;
  cur_index++;
  return cur_record;
}
 
/**
 * return false if iterator reaches the end
 * of the run
 */
bool RunIterator::has_next() {
  if(cur_index < run_length) return 1;
  else return 0;
}

void merge_runs(RunIterator* iterators[], int num_runs, FILE *out_fp,
                long start_pos, char *buf, long buf_size)
{
  // Your implementation
  fseek(out_fp, start_pos, SEEK_SET);
  int cur_buf_count = 0;
  int bytes_per_record = iterators[0]->schema->bytes_per_record();
  int record_nums = buf_size/bytes_per_record;
  priority_queue <RunIterator*, vector<RunIterator*>, Compare> heap;
  
  for(int i = 0; i < num_runs; i++){
    iterators[i]->next();
    heap.push(iterators[i]);
  }
  while (!heap.empty()){
    RunIterator* it = heap.top();
    heap.pop();
    int write_index = cur_buf_count*(bytes_per_record+1);
    memcpy((char*) buf + write_index, it->cur_record->data, bytes_per_record);
    memcpy((char*) buf + write_index + bytes_per_record, "\n", 2);
    cur_buf_count += 1;
    if (cur_buf_count == record_nums){
      fprintf(out_fp, (char*)buf);
      cur_buf_count = 0;
    }
    if (it->has_next()){
      it->next();
      heap.push(it);
    }
  }
  if (cur_buf_count != 0){
    fprintf(out_fp, (char*)buf);
  }
}

