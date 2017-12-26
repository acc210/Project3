#include "sorter_server.h"

/*
    **************************************
	 			my_mergesort
	**************************************
	db is a struct. The rows are stored in db->entries, which is an array of pointers to Entry structs, each entry has the data in entry->items[i], which is an array of pointers of type item_ptr, which point to the data. This function receives a pointer to db. "col" is the index of the column to be sorted by: db->entries[i][col] is the data you will use to sort. "type" is the type of the data in that column (see line 26). "start" and "stop" are the indices for merge sort. The recursion is being called on line 37, inside the if statement.
	my_mergesort returns 1 if successful, 0 if something goes wrong.
*/


void my_mergesort(Db *db, int start, int stop){
	int range = stop-start;
	if(range<=0){ // base case or error
		return;
	}


	//assign compare function based on type of data for column
	int (*compare)(item_ptr,item_ptr);
	if(type=='i'){
		compare = int_compare;
	}else if(type=='f'){
		compare = float_compare;
	}else{
		compare = str_compare;
	}

	int midpoint = start+(range)/2;
	//sort both halves, return if either fails:
	my_mergesort(db, start, midpoint);
	my_mergesort(db,  midpoint+1, stop);

	Entry *temparr[range+1];
	int i, j, k;
	Db* which_db = db;
	int start_from = start;
	for(i=0; i<=range; i++){
		while((start_from+i)>=MAX_DB_SIZE){
			which_db = which_db->next;
			start_from -= MAX_DB_SIZE;
		}
		temparr[i] = which_db->entries[start_from+i];
	}
	which_db = db;

	for(i=0, j=midpoint-start+1, k=start; j<=range && i<midpoint-start+1 && k<=stop;){
		while(k>=MAX_DB_SIZE){
			which_db = db->next;
			k -= MAX_DB_SIZE;
		}
		if(compare(temparr[i]->items[col_num], temparr[j]->items[col_num])<1){
			which_db->entries[k] = temparr[i];
			i++;
			k++;
		}else{
			which_db->entries[k] = temparr[j];
			j++;
			k++;
		}
	}

	while(i<midpoint-start+1){
		which_db->entries[k] = temparr[i];
		i++;
		k++;
	}
	while(j<=range){
		which_db->entries[k] = temparr[j];
		j++;
		k++;
	}

	return;
}


Db* merge_DBs(Db *db1, Db *db2){

	Db *merged_db = (Db*)malloc(sizeof(Db));
	merged_db->num_rows = db1->num_rows + db2->num_rows;
	merged_db->entries = (Entry**)malloc(merged_db->num_rows*sizeof(Entry*));
	merged_db->next = NULL;

	int (*compare)(item_ptr, item_ptr);
	if(type=='i'){
		compare = int_compare;
	}else if(type=='f'){
		compare = float_compare;
	}else{
		compare = str_compare;
	}

	int a, b, i;
	a=b=i=0;
	while(a < db1->num_rows && b< db2->num_rows && i<MAX_DB_SIZE){
		if(compare(db1->entries[a]->items[col_num], db2->entries[b]->items[col_num])>0){
			merged_db->entries[i] = db2->entries[b];
			i++;
			b++;
		}else{
			merged_db->entries[i] = db1->entries[a];
			i++;
			a++;
		}

	}
	while(a<db1->num_rows){
		merged_db->entries[i] = db1->entries[a];
		i++;
		a++;
	}
	while(b<db2->num_rows){
		merged_db->entries[i] = db2->entries[b];
		i++;
		b++;
	}

	free(db1);
	free(db2);
	done_merging(merged_db);
	
	return merged_db;

}

void done_merging(Db* db){
	pthread_mutex_lock(&mutex_add_merged);
	if(master_db == NULL){
		master_db = db;
		master_in_use = 1;
		pthread_mutex_lock(&mutex_increment_running_threads);
		running_threads--;
		pthread_mutex_unlock(&mutex_increment_running_threads);
		pthread_mutex_unlock(&mutex_add_merged);
		send_sorted_db();
		pthread_exit(0);

	}else{
		Db *temp = master_db;
		master_db = NULL;
		master_in_use = 0;
		pthread_mutex_unlock(&mutex_add_merged);
		merge_DBs(temp, db);
	}
	return;
}



int int_compare(item_ptr pa, item_ptr pb){
	int a = *(pa.i);
	int b = *(pb.i);
	if(a==b) return 0;
	else if(a<b) return -1;
	else return 1;

}

int float_compare(item_ptr pa, item_ptr pb){
	float a = *(pa.f);
	float b = *(pb.f);
	if(a==b) return 0;
	else if(a<b) return -1;
	else return 1;

}

int str_compare(item_ptr pa, item_ptr pb){
	char* a = *pa.s;
	char* b = *pb.s;
	if(a[0]=='\"') a++;	//skip quotes when applicable
	if(b[0]=='\"') b++;
	return strcmp(a, b);
}