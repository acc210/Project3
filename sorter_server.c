#include "sorter_server.h"

/*
***************************************************
Globals
***************************************************
*/
//corresponding data types
const char types[NUM_COLS] = {'s', 's', 'i', 'i', 'i', 'i', 's', 'i', 'i', 's', 's', 's', 'i', 'i', 's', 'i', 's', 's', 'i', 's', 's', 's', 'i', 'i', 'i', 'f', 'f', 'i'};

Db *master_db;
pthread_mutex_t mutex_add_merged = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_print_pids = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_increment_running_threads = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t file_id_mutex;
int file_id = 0;
int server_fd;


int main(int argc, char * argv[]){
	signal(SIGSEGV, segf);
	signal(SIGKILL, segv);
	signal(SIGINT, segv);
	master_db = NULL;
	running_threads = 0;
	master_in_use = 0;
	if(argc<3){
		perror("Error expected: ./sorter_server -p <port_num>\n");
		return 0;
	}

	sort_col = NULL;
	threads_index=0;
	port = atoi(argv[2]);

    serverStart(port);
	return 1;

}


void serverStart(int port){
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    int new_socket, *soc;

    // Creating socket file descriptor
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0){
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    // Forcefully attaching socket to the port 8080
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address))<0){
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(server_fd, 3) < 0){
        perror("listen");
        exit(EXIT_FAILURE);
    }

    printf(".\n");
    while(1){
    	// loop forever, accepting requests and threading for each
    	printf(".\n");
		if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen))>0){ //&& !running_threads
			soc = (int*)malloc(sizeof(int));
			*soc = new_socket;
			pthread_mutex_lock(&mutex_increment_running_threads);
			pthread_create(&threads[threads_index++], NULL, server_read, (void*)soc);
			running_threads++;
			pthread_mutex_unlock(&mutex_increment_running_threads);
		}
	}
}

int basicStart() {

    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    int server_fd, new_socket, *soc;

    // Creating socket file descriptor
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0){
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);


    if (listen(server_fd, 3) < 0){
        perror("listen");
        exit(EXIT_FAILURE);
    }

    if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen))>0){

			return new_socket;
    }

    return -1;

}



void sendServer(int new_socket) {  

//	printf("IN SERVER SEND\n");
	send_sorted_db();
	char* path = "./f-sorted-.csv";

	char buff[1024] = "";

	FILE *fp = fopen(path, "r");
	if(fp == NULL)
	{
		fprintf(stderr, "ERROR: File %s not found\n", path);
		exit(1);
	}

	memset(&buff, 0, sizeof(buff));
	size_t elements; //number of elements from stream that fread has read
	while((elements = fread(buff, sizeof(char), 1024, fp)) > 0)
	{
		if(send(new_socket, buff, elements, 0) < 0)
	    {
			fprintf(stderr, "ERROR: Send File Unsuccesful");
		    break;
	    }

	   memset(&buff, 0, sizeof(buff));

		}


	fclose(fp);
	close(new_socket);

}

int checkMeta(char* metadata, int new_socket) {


    if(metadata[0] == '-') {  /// if "-" then send file back to client
        pthread_mutex_lock(&mutex_increment_running_threads);
        running_threads--;
        pthread_mutex_unlock(&mutex_increment_running_threads);
         sendServer(new_socket);
        return -1;
    }

    /******* if "+" is sent; find col number ***/
    int i = 1;

    char buff[30] = "";
    while(metadata[i] != '@') {

        buff[i-1] = metadata[i];
        i++;
    }



    return atoi(buff);

}


void* server_read(void *soc){

	int new_socket = *((int*)soc);
    char metadata[1024] = "";
    recv(new_socket, metadata, 1024, 0);

    int col_number = checkMeta(metadata,new_socket);

    if(col_number == -1) {

        return 0;

    }

    col_num = col_number;

	char* path = (char*)malloc(100);
	char *fnum = (char*)malloc(100);
	strcpy(path, "./");
	pthread_mutex_lock(&file_id_mutex);
	sprintf(fnum, "%u", file_id++);
	pthread_mutex_unlock(&file_id_mutex);
	strcat(path, fnum);
    strcat(path,"-sorted-");
	strcat(path, ".csv");
	char buff[1024] = "";         
	FILE *fp = fopen(path, "w");

	if(fp == NULL)
	{
		fprintf(stderr, "ERROR: File %s not found\n", path);
		exit(1);
	}

	memset(&buff, 0, sizeof(buff));
	ssize_t elements = 0;

	int i, data_index_start=3;
	for(i=0; i<strlen(metadata)-1; i++){
		if(metadata[i]=='@' && metadata[i+1]=='@'){
			data_index_start = i+2;
			break;
		}
	}
	char *data = (char*)malloc(1024);
	strcpy(data, metadata+data_index_start);

	fwrite(data, sizeof(char), strlen(data), fp);
	while((elements = recv(new_socket, buff, 1024, 0)) > 0)  //read into buffer
	{
 		int i;

 		 for(i = 0; i < 1024; i++) {
            if(buff[i] == '@') {
                buff[i] = '\0';
                elements-=2;

            }
 		 }

		int numWrite = fwrite(buff, sizeof(char), elements, fp);
		if(numWrite < elements)
	    {
			fprintf(stderr, "ERROR: failed to write file \n");
		}


	memset(&buff, 0, sizeof(buff));

	}

	if(elements < 0)
	{
		fprintf(stderr, "ERROR: failed to recieve contents");
		exit(1);
	}

 //   printf("File Recived Succesfully\n"); //keep for testing

    fclose(fp);
    fp = fopen(path, "r");
//    printf("298 path:%s\n", path);
    sort_csv(fp);

	return 0;
}

void send_sorted_db(void){
	while(running_threads>0){ //wait until all threads are done
		;
	}

	FILE *f = fopen("f-sorted-.csv", "w");
	print_db(master_db, f);
	fclose(f);
}


void* sort_csv(FILE *fp){

	Db *db = make_new_db();
	populate_db(db, fp);
	fclose(fp);

	my_mergesort(db, 0, db->num_rows-1);

	done_merging(db);

	return 0;
}

//	change to read from socket instead of file
int populate_db(Db *db, FILE* fp){
	int row, col, i, j, in_quotes=0;
	int next = 0;
	int o = 1;
	char c;
	char *str = NULL;
	db->num_rows=0;
	for(row=0; row<MAX_DB_SIZE; row++){	//for each row(entry)
		if(o>0){
			db->num_rows++;
			// instantiate Entry
			db->entries[row] = make_new_entry();
			for(col=0; col<NUM_COLS; col++){	//for each col
				str = (char*)calloc(MAX_STR, sizeof(char));
				db->entries[row]->items[col].s = (char (*)[MAX_STR])str;
				in_quotes = 0;
				for(i=0; i<MAX_STR && (o=fscanf(fp, "%c", &c))>0 && c!='\0' && (c!=',' || in_quotes==1); i++){
					if(c=='"'){
						if(in_quotes)
							in_quotes = 0;
						else
							in_quotes = 1;
					}else if(c=='\n' || c=='\r'){  //make next entry
						if(col==0){ 
							dealloc_entry(db->entries[row]);
							printf("263 num_rows: %d\n", db->num_rows);
							db->num_rows--; //getting rid of return at the end of the file
							printf("263 num_rows: %d\n", db->num_rows);
							goto Done; 
						}else if(col<NUM_COLS-1){ /
							dealloc_db(db);
							fclose(fp);
							pthread_exit(0);
						}
						str[i] = '\0';
						next++;
						goto Next_Row;	//go to next row in current db, line 332
					}else if(c==',' && !in_quotes){
						if(type=='s') str[i] = '\0';
						col++;
						break;	//go to next column in current entry
					}
					str[i] = c;
				}
				if(o<=0){
					if(col==0){
						db->num_rows--;
					}
					goto Done;
				}

			if(types[col]=='s'){ //clear trailing whitespace
					while(i>0 && (str[--i]==' '||str[i]=='\t')){
					}
					str[++i]='\0';
					i--;
					if(i>0 && (str[i]=='\"')){ //clear trailing whitespace inside quotes
						while(str[--i]==' '||str[i]=='\t'){
							str[i] = '\"';
							str[i+1] = '\0';
						}
					}
				}
				if(types[col]=='i'){
					int* num = (int*)malloc(sizeof(int));
					*num = atoi(str);
					db->entries[row]->items[col].i = num;
					free(str);
				}else if(types[col]=='f'){
					float* num = (float*)malloc(sizeof(float));
					*num = atof(str);
					db->entries[row]->items[col].f = num;
					free(str);
				}
			}
			Next_Row:
			i++;
		}else{
			db->entries[row]=NULL;
			return 1;
		}
	}
	Done:
	if(row==MAX_DB_SIZE && o){
		db->next = (Db*)malloc(sizeof(Db));
		db->next->next = NULL;
		populate_db(db->next, fp);
	}
	return 1;
}


//Folded section :create/print/dealloc Db:
void print_cols(FILE* fp){
	int i;
	for(i=0; i<NUM_COLS; i++){
		if(i<NUM_COLS-1) fprintf(fp, ",");
	}
	fprintf(fp, "\n");
	return;
}

void print_db(Db *db, FILE* fp){ //prints whole database, one entry on each line
	print_rows(db, fp);
	if(db->next){
		print_rows(db->next, fp);
	}
	return;
}

void print_rows(Db *db, FILE* fp){
	int i;
	for(i=0; i<db->num_rows && db->entries[i]!=NULL; i++){//&& i<MAX_DB_SIZE
		print_row(db, db->entries[i], fp);
		if(i<(db->num_rows)-1) fprintf(fp, "\n");
	}

	return;
}

void print_row(Db *db, Entry *row, FILE* fp){	//prints contents of Entry (row) into one line
	int i, r;
	for(i=0, r=1; i<NUM_COLS && row->items[i].s!=NULL;){
		if(types[i]=='i'){
			fprintf(fp, "%d", *((row->items[i]).i));
		}else if(types[i]=='f'){
			fprintf(fp, "%1.3f", *((row->items[i]).f));
		}else{
			if(*((row->items[i]).s)!=0)
				fprintf(fp, "%s", *((row->items[i]).s));
		}
		++i;
		if(row->items[i].s==NULL || (i==NUM_COLS && row->next==NULL && ++r<db->num_rows))
			;
		else
			fprintf(fp, ",");
	}

	if(row->next){
		fprintf(fp, "\n");
		print_row(db, row->next, fp);
	}
}

Db* make_new_db(void){
	Db *db = (Db*)malloc(sizeof(Db));
	db->num_rows = 0;
	db->entries = (Entry**)malloc(MAX_DB_SIZE*sizeof(Entry*));
	db->next = NULL;

	return db;
}

Entry* make_new_entry(void){

	Entry *entry = (Entry*)malloc(sizeof(Entry));
	entry->items = (item_ptr*)malloc(NUM_COLS*sizeof(item_ptr)+50);
	entry->next = NULL;

	return entry;
}

int dealloc_entry(Entry *entry){
	if(entry->next)
		dealloc_entry(entry->next);
	free(entry->items);
	free(entry);
	return 0;
}

int dealloc_db(Db *db){
	int i, j;
	if(db->next) {
		dealloc_db(db->next);
		free(db->next);
	}

	for(i=0; i<MAX_DB_SIZE && i<db->num_rows; i++){
			if(db->entries[i]){
				dealloc_entry(db->entries[i]);
		}
	}
	free(db);
	return 0;
}

void print_types(Db *db){
	int i;
	for(i=0; i<NUM_COLS; i++){
		printf("%c: %c  ", 'a'+i, type);
	}
	printf("\n");
}

void segf(int i){ //handles seg faults
	printf("Thread ID:%d caused a segfault. Program exiting.\n", (int)pthread_self());
	exit(0);
}

void segv(int i){ //handles seg faults
	printf("\nSig interrupt\n");
	close(server_fd);
	exit(0);
}