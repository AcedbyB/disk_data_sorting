#include "library.h"
#include <iostream>
#include <string.h>
#include <stdlib.h>  
using namespace std;

int compare (const void * a, const void * b)
{
  Record *rA = *(Record **)a;
  Record *rB = *(Record **)b;
  char str_yearA[5];
  char str_yearB[5];
  memcpy(str_yearA, rA -> data + 19, 4 );
  memcpy(str_yearB, rB -> data + 19, 4 );
  str_yearA[4] = '\0';
  str_yearB[4] = '\0';
  int nA = atoi(str_yearA);
  int nB = atoi(str_yearB);

  if ( nA < nB ) return -1;
  if ( nA == nB ) return 0;
  if ( nA > nB ) return 1;
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
    fgets (buffer, 1 , in_fp); //skips beginning of file character
    if ( fgets (records[cur] -> data , bytes_per_record + 1, in_fp) == NULL ) break;
    fgets (buffer, 2 , in_fp); //skips end of line character
    cur++;

    if(cur == run_length) {
      qsort (records, run_length, sizeof(Record*), compare);
      for(int i = 0; i < run_length; i++) {
        cout<<records[i] -> data<<endl;
      }
      cur = 0;
      cout<<endl;
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

