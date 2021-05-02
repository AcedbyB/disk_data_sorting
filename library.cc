#include "library.h"
#include <iostream>
#include <string.h>
#include <stdlib.h>  
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

void mk_runs(FILE *in_fp, FILE *out_fp, long run_length, Schema *schema)
{
  int cur = 0;
  char buffer[50];
  Record* records[run_length];
  for(int i = 0; i < run_length; i++) {
    records[i] = new Record();
    records[i] -> schema = schema;
  }

  int bytes_per_record = schema -> nattrs;
  for(int i = 0; i < schema -> nattrs; i++) bytes_per_record += schema -> attrs[i] -> length;
  while ( !feof (in_fp) ) {
    fgets (buffer, 1 , in_fp);
    if ( fgets (records[cur] -> data , bytes_per_record + 1, in_fp) == NULL ) break;
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

void merge_runs(RunIterator* iterators[], int num_runs, FILE *out_fp,
                long start_pos, char *buf, long buf_size)
{
  // Your implementation
}

