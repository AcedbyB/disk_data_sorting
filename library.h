#include <cstdio>

/**
 * An attribute schema. You should probably modify
 * this to add your own fields.
 */
typedef struct {
  char name[20];
  int length;
  char type[20];
  char distribution[20];
} Attribute;

/**
 * A record schema contains an array of attribute
 * schema `attrs`, as well as an array of sort-by 
 * attributes (represented as the indices of the 
 * `attrs` array).
 */
typedef struct {
  Attribute * attrs[5];
  int nattrs;
  int sort_attrs[5];
  int n_sort_attrs;
  int offset[5];
} Schema;

/**
 * A record can defined as a struct with a pointer
 * to the schema and some data. 
 */
typedef struct {
  Schema* schema;
  char data[50];
} Record;

/**
 * Creates sorted runs of length `run_length` in
 * the `out_fp`.
 */
void mk_runs(FILE *in_fp, FILE *out_fp, long run_length, Schema *schema);

/**
 * The iterator helps you scan through a run.
 * you can add additional members as your wish
 */
class RunIterator {
  /**
   * Creates an interator using the `buf_size` to
   * scan through a run that starts at `start_pos`
   * with length `run_length`.
   */

  public:
  
  long start_pos, run_length, buf_size;
  Schema *schema;
  FILE *fp;
  int cur_index = 0;
  Record *cur_record;

  RunIterator(FILE *fp, long start_pos, long run_length, long buf_size,
              Schema *schema);

  /**
   * free memory
   */
  ~RunIterator();

  /**
   * reads the next record
   */
  Record* next();

  /**
   * return false if iterator reaches the end
   * of the run
   */
  bool has_next();
};

/**
 * Merge runs given by the `iterators`.
 * The number of `iterators` should be equal to the `num_runs`.
 * Write the merged runs to `out_fp` starting at position `start_pos`.
 * Cannot use more than `buf_size` of heap memory allocated to `buf`.
 */
void merge_runs(RunIterator* iterators[], int num_runs, FILE *out_fp,
                long start_pos, char *buf, long buf_size);

