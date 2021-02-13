def get_all_columns(table_name)
    if table_name.end_with?('.csv')
      @file_name = table_name
    else
      @file_name = table_name+'.csv'    
    end 
      data_array = CSV.parse(File.read(@file_name))
      cols = data_array.shift
      return cols
  end
  
  def create_table(column_titles, data)
    data_table = []  
    data.each do |row|
      table_row = {}
      row.each_index do |index|
        table_row[:"#{column_titles[index]}"] = "#{row[index]}"
      end
      data_table.push(table_row)
    end
    
    return data_table
  end

  def filter_query_response
    join_data = false
    table2_hash = []      
    @data_table.each do |data_hash| 
      next if @where_filter.length > 0 && (data_hash[:"#{@where_filter[0]}"] != @where_filter[1])
      next if @where_count>1 && (data_hash[:"#{@where_filter[2]}"] != @where_filter[3])      
      if @join_tables.length > 0
        col1 = data_hash[:"#{@join_tables[0]}"]        
        @table_2.each do |data_hash_join|         
          if data_hash_join[:"#{@join_tables[1]}"] == col1 && !table2_hash.include?(data_hash_join)           
            table2_hash.push(data_hash_join)           
            join_data = true
          end
        end
      end
      next if @join_tables.length > 0 && join_data == false
      joined_rows = []
      response_hash = {}
      @select_cols.each do |column|
        response_hash[:"#{column}"] = data_hash[:"#{column}"]
      end      
      @query_response.push(response_hash)      
    end
    if join_data == true
      joined_rows = filter_joined_rows(table2_hash)
      index=0
      @query_response.each do |data_hash|  
        data_hash[:"#{@column_to_join}"]=joined_rows[index][:"#{@column_to_join}"]
        index+=1
      end 
    end   
    if @sort_data.length > 0
      if @sort_data[0] == 'ASC'
        @query_response = @query_response.sort_by{ |hash| hash[:"#{@sort_data[1]}"] }
      end
      if @sort_data[0] == 'DESC'
        @query_response = @query_response.sort_by{ |hash| hash[:"#{@sort_data[1]}"] }.reverse
      end
    end
  end

  def filter_joined_rows(table2_hash)   
    matched_data = []
    table2_hash.each do |row|     
      matched_columns = {}
      @select_cols.each do |column_name|       
        if row[:"#{column_name}"]
          matched_columns[:"#{column_name}"] = row[:"#{column_name}"]
          @column_to_join=column_name
        end
      end
      matched_data.push(matched_columns)     
    end
    return matched_data
  end

  def insert_data
    columns = @insert_data.keys
    new_row = []
    @all_columns.each_index do |index|     
      if !columns.include?(@all_columns[index])
        new_row[index] = ','      
      else
        col = @all_columns[index].to_s
        if (index==6)
          new_row[index] = '"'+@insert_data["#{col}"] +'"'
        else
          new_row[index] = @insert_data["#{col}"]
        end
      end
    end
    File.write(@file_name, "\n", mode: "a")
    File.write(@file_name, new_row.join(","), mode: "a")
  end

  def update_table
    keys=[]
    keys = @update_data.keys
    #print keys
    @data_table.each do |data_hash|
      @query_response.each_index do |index|
        data_hash_response = query_response[index]
        if data_hash[:"#{@where_filter[0]}"] == data_hash_response[:"#{@where_filter[0]}"]
          keys.each do |key|  
            data_hash[:"#{key}"] = @update_data[:"#{key}"]
            query_response[index][:"#{key}"] = @update_data[:"#{key}"]
          end
        end
      end
    end
  end

  def update_file
    new_rows = []
    @data_table.each do |data_hash|
      if (data_hash[:birth_date])
        data_hash[:birth_date]='"'+data_hash[:birth_date]+'"'
      end
      values = data_hash.values
      new_rows.push(values)
    end  
    File.write(@file_name, @all_columns.join(","))
    File.write(@file_name, "\n", mode: "a")
    new_rows.each_index do |index|
      File.write(@file_name, new_rows[index].join(","), mode: "a")
      if index < new_rows.length-1
        File.write(@file_name, "\n", mode: "a")
      end
    end
  end

  def delete_data
    @data_table.each_index do |index|
      if @where_filter.length == 0
        @data_table.delete_at(index)
      else        
        data_hash = @data_table[index]
        #print data_hash
        if data_hash[:"#{@where_filter[0]}"] == @where_filter[1]
          @data_table.delete_at(index)
        end
      end
    end
  end
