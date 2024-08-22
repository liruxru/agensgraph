/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "postgres.h"

#include "utils/ag_load_edges.h"
#include "utils/csv.h"


static void setVertex(char** vertex, char* oriname);
void edge_field_cb(void *field, size_t field_len, void *data)
{

    csv_edge_reader *cr = (csv_edge_reader*)data;
    if (cr->error)
    {
        cr->error = 1;
        ereport(NOTICE,(errmsg("There is some unknown error")));
    }

    // check for space to store this field
    if (cr->cur_field == cr->alloc)
    {
        cr->alloc *= 2;
        cr->fields = (char**)realloc(cr->fields, sizeof(char *) * cr->alloc);
        cr->fields_len = (size_t*)realloc(cr->header, sizeof(size_t *) * cr->alloc);
        if (cr->fields == NULL)
        {
            cr->error = 1;
            ereport(ERROR,
                    (errmsg("field_cb: failed to reallocate %zu bytes\n",
                            sizeof(char *) * cr->alloc)));
        }
    }
    cr->fields_len[cr->cur_field] = field_len;
    cr->curr_row_length += field_len;
    cr->fields[cr->cur_field] = strndup((char*)field, field_len);
    cr->cur_field += 1;
}
static void setVertex(char** vertex, char* oriname){
    char *start_lab = strchr(oriname, (int)'(');
    char *end_lab = strchr(oriname, (int)')');
    char dest[100];
    memcpy(dest, start_lab + 1, end_lab - start_lab-1);
    dest[end_lab - start_lab-1] = '\0'; 
    char *temp = (char *)palloc(100);
    memset(temp,0,100);
    strcpy(temp,dest);
    int length = strlen(temp);
    
    for (int i = 0; i < length; i++) {
        temp[i] = tolower(temp[i]);
    }
    *vertex = temp;
}


// Parser calls this function when it detects end of a row
void edge_row_cb(int delim __attribute__((unused)), void *data, Relation label_relation)
{

    csv_edge_reader *cr = (csv_edge_reader*)data;

    size_t i, n_fields;
    int64 start_id_int;
    Graphid start_vertex_graph_id;
    uint16 start_vertex_type_id;

    int64 end_id_int;
    Graphid end_vertex_graph_id;
    uint16 end_vertex_type_id;

    Graphid object_graph_id;

    Jsonb* props = NULL;

    n_fields = cr->cur_field;

    if (cr->row == 0)
    {
        cr->header_num = cr->cur_field;
        cr->header_row_length = cr->curr_row_length;
        cr->header_len = (size_t* )malloc(sizeof(size_t *) * cr->cur_field);
        cr->header = (char**)malloc((sizeof (char*) * cr->cur_field));
        cr->col_type = (enum jbvType *)malloc((sizeof (enum jbvType) * cr->cur_field));
        for ( i = 0; i<cr->cur_field; i++)
        {
            cr->header_len[i] = cr->fields_len[i];
            char *start = NULL;
            char dest[100];
            char type[100];

            // creationDate:LONG|:START_ID(Person)|:END_ID(Organisation)|workFrom:LONG
             char *startid =  strstr(cr->fields[i], "START_ID"); 
             if(startid) {
                cr->start_col = i;
                cr->header[i] = "end_id";
                setVertex(&cr->start_vertex,cr->fields[i]);
             } 
             char *endid =  strstr(cr->fields[i], "END_ID"); 
             if(endid) {
                setVertex(&cr->end_vertex,cr->fields[i]);
                cr->header[i] = "start_id";
                cr->end_col = i;
             } 
             if(startid || endid){
                cr->col_type[i] = jbvNumeric;
                continue;
             }
           

            start = strchr(cr->fields[i], (int)':');         // 找到字符':'的位置
            if(start){
                memcpy(dest, cr->fields[i], start - cr->fields[i]);
                dest[start - cr->fields[i]] = '\0';        // 将字符串dest的最后一个字符']'改成'\0'，如果最后一个字符不是'\0'的话，那么在该字符串的最后一位是乱码的
          

                memcpy(type, start + 1, cr->fields[i]+strlen(cr->fields[i])-start);// 将字符串s中'['之后的所有内容都copy出来包括字符']'，这是为了之后的分割字符串使用的
                type[ cr->fields[i]+strlen(cr->fields[i])-start-1] = '\0';
                cr->header[i] = strndup(dest, strlen(dest));

                if(strcmp(type, "LONG") == 0){
                    cr->col_type[i] = jbvNumeric;
                }else  if(strcmp(type, "STRING") == 0){
                    cr->col_type[i] = jbvString;
                }else  if(strcmp(type, "BOOL") == 0){
                    cr->col_type[i] = jbvBool;
                }else{
                    cr->col_type[i] = jbvString;
                }
            }else{
                cr->header[i] = strndup(cr->fields[i], cr->header_len[i]);
            }
           
        }
        cr->start_vertex_type_id = get_labname_labid(cr->start_vertex, cr->graph_id);
        cr->end_vertex_type_id = get_labname_labid(cr->end_vertex, cr->graph_id);
        ereport(NOTICE, (errmsg("load_edge: %s_%s", cr->start_vertex,cr->end_vertex)));
    }
    else
    {
        GraphidSet( &object_graph_id, cr->object_id, (int64)cr->row);

        start_id_int = strtol(cr->fields[ cr->start_col], NULL, 10);
        char* start_label_name =  cr->start_vertex;
        // 获取起点labid
        start_vertex_type_id = cr->start_vertex_type_id ;
        end_id_int = strtol(cr->fields[cr->end_col], NULL, 10);

        char* end_label_name = cr->end_vertex;
        // 获取终点labid
        end_vertex_type_id = cr->end_vertex_type_id;
        
        GraphidSet(&start_vertex_graph_id,start_vertex_type_id, start_id_int+1);
        GraphidSet(&end_vertex_graph_id,end_vertex_type_id, end_id_int+1);

        props = create_agtype_from_list_i(cr->header, cr->fields,cr->col_type,
                                          n_fields, cr->start_col,cr->end_col);

        insert_edge_simple(cr->graph_id, cr->object_name,
                           object_graph_id, start_vertex_graph_id,
                           end_vertex_graph_id, props );
 
    }


    for (i = 0; i < n_fields; ++i)
    {
        free(cr->fields[i]);
    }

    if (cr->error)
    {
        ereport(NOTICE,(errmsg("THere is some error")));
    }


    cr->cur_field = 0;
    cr->curr_row_length = 0;
    cr->row += 1;
}

static int is_space(unsigned char c)
{
    if(c == CSV_SPACE || c == CSV_TAB) return 1;
    return 0;
}

static int is_term(unsigned char c)
{
    if (c == CSV_CR || c == CSV_LF) return 1;
    return 0;
}

int create_edges_from_csv_file(char *file_path,
                               char *graph_name,
                               Oid graph_id,
                               char *object_name,
                               int object_id )
{

    FILE *fp;
    struct csv_parser p;
    char buf[1024];
    size_t bytes_read;
    unsigned char options = 0;
    csv_edge_reader cr;

    if (csv_init(&p, options) != 0)
    {
        ereport(ERROR,
                (errmsg("Failed to initialize csv parser\n")));
    }

    csv_set_space_func(&p, is_space);
    csv_set_term_func(&p, is_term);

    fp = fopen(file_path, "rb");
    if (!fp)
    {
        ereport(ERROR,
                (errmsg("Failed to open %s\n", file_path)));
    }


    memset((void*)&cr, 0, sizeof(csv_edge_reader));
    cr.alloc = 128;
    // changed by songwenbin
    //cr.fields = malloc(sizeof(char *) * cr.alloc);
    cr.fields = (char**)malloc(sizeof(char *) * cr.alloc);
    // changed by songwenbin
    //cr.fields_len = malloc(sizeof(size_t *) * cr.alloc);
    cr.fields_len = (size_t*)malloc(sizeof(size_t *) * cr.alloc);
    cr.header_row_length = 0;
    cr.curr_row_length = 0;
    cr.graph_name = graph_name;
    cr.graph_id = graph_id;
    cr.object_name = object_name;
    cr.object_id = object_id;

 
                       
    while ((bytes_read=fread(buf, 1, 1024, fp)) > 0)
    {
        if (csv_parse(&p, buf, bytes_read, edge_field_cb,
                      edge_row_cb, &cr) != bytes_read)
        {
            ereport(ERROR, (errmsg("Error while parsing file: %s\n",
                                   csv_strerror(csv_error(&p)))));
        }
    }


    csv_fini(&p, edge_field_cb, edge_row_cb, &cr);
 
    if (ferror(fp))
    {
        ereport(ERROR, (errmsg("Error while reading file %s\n", file_path)));
    }

    fclose(fp);

    free(cr.fields);
    free(cr.fields_len);
    csv_free(&p);
    return EXIT_SUCCESS;
}
