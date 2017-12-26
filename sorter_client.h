#ifndef SORTER_H
#define SORTER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/wait.h>
#include <pthread.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#define PORT 8080

#define MAX_DB_SIZE 10000
#define MAX_STR 300
#define NUM_COLS 28 //original number of columns
#define MAX_COLS 300
#define MAX_THREADS 2500
//#define PATH_MAX 300

int main (int argc, char **argv);
void *direcFound(void* ptr);
void *csvFound(void* ptr);
void sendClient(char* path);
void startClient(char* hostName, int port);
void *direcFound(void* ptr);
void requestFile(char* host, int port, char* path);
#ifndef info_H
#define info_H
typedef struct info {
    char* path;
	char* file;
}in;

#endif

/*
***************************************************
Macro Constants and Includes
***************************************************
*/




/*
***************************************************
Structs, Unions, Typedefs
***************************************************
*/

typedef	//entry (row) has an array of these that points to data
union _item_ptr{
	char (*s)[MAX_STR];	//pointer to array of 300 chars
	int* i;
	float* f;
}item_ptr;

typedef
struct _entry{
	item_ptr *items;
	struct _entry *next;	//used for overflow
}Entry;

typedef
struct _db{
	int num_rows;
	Entry ** entries;
	struct _db *next;	//used for overflow
}Db;

typedef
struct _dirargs{ //argument struct for process_dir()
	char *dir_name;
// 	char *sort_col;
} dir_args;

typedef
struct _sortargs{//argument struct for sort_csv()
	char *file_path;
	char *filename;
} sort_args;

/*
***************************************************
Global Variables
***************************************************
*/

extern Db *master_db;
extern pthread_mutex_t mutex_increment_running_threads, mutex_add_merged;
int col_num, threads_index;
char type;
int running_threads, master_in_use;
char *sort_col, *out_dir;
pthread_t threads[MAX_THREADS];
int port;

/*
***************************************************
functions in sorter.c
***************************************************
*/

void* process_dir(void*); //scans dir for dirs and .csv, threads when found
void* sort_csv(FILE *fp); //receives sort_args struct, sorts csv, calls done_merging()
int populate_db(Db *db, FILE* fp); //reads all data available in stdin into internal db
int determine_sort_col(char * str); //returns index of column name str, -1 if not present
void serverStart(int port);
void start_connection(void *soc);
void* server_read(void *soc);

void print_cols(FILE* fp);	//prints out column titles
void print_db(Db *db, FILE* fp);	//prints column titles, then calls print row on every row
void print_rows(Db *db, FILE* fp);
void print_row(Db *db, Entry *row, FILE* fp);	//prints contents of Entry (row) into one line
Db* make_new_db(void); //file_path includes path and file name. filename is str of filename.csv
Entry* make_new_entry(void);
int dealloc_entry(Entry *entry);
int dealloc_db(Db *db);	//deallocs all linked databases, every entry and every pointer in each
void print_types(Db *db); //for debugging, prints out the types of data in each column
int write_csv(Db *db, FILE *fdest);
void segf(int); //handles seg faults

/*
***************************************************
functions in mergesort.c
***************************************************
*/

void my_mergesort(Db *db, int start, int stop);
Db* merge_DBs(Db *db1, Db *db2); //merges 2 merged DBs, returns result
void done_merging(Db*); //each thread calls this with its sorted db when it's done merging
void increment_yoself(void); //used by threads to increment global running threads count
void decrement_yoself(void); //used by threads to decrement global running threads count
int int_compare(item_ptr pa, item_ptr pb);
int float_compare(item_ptr pa, item_ptr pb);
int str_compare(item_ptr pa, item_ptr pb);


#endif