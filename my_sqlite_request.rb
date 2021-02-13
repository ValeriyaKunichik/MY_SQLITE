require 'csv'
require "./methods.rb"

class MySqliteRequest

  def initialize
    @data_table = nil
    @file_name =nil     
    @table_2 =nil       
    @all_columns = []     
    @select_cols = [] 
    @where_filter = [] 
    @where_count = 0   
    @join_tables = [] 
    @sort_data = []  
    @insert_new_row = false 
    @insert_data = {} 
    @update_data = {} 
    @delete_data = false 
    @query_response = [] 
    class << self
      attr_accessor :query_response
    end
  end

  def from(table_name)
    if table_name.end_with?('.csv')
      @file_name = table_name
    else
      @file_name = table_name+'.csv'    
    end   
    return self
  end

  def select(column_name) 
    if column_name.kind_of?(Array)
      @select_cols = column_name
    else
      @select_cols.push(column_name)
    end
    return self
  end

  def where(column_name, criteria)
    @where_count+=1
    @where_filter.push(column_name)
    @where_filter.push(criteria)
    return self
  end

  def join(column_on_db_a, filename_db_b, column_on_db_b)
    if !filename_db_b.end_with?('.csv')
      filename_db_b = filename_db_b+'.csv'   
    end 

    data_arr_b = CSV.parse(File.read(filename_db_b))   
    column_titles_b = data_arr_b.shift   
    @table_2 = create_table(column_titles_b, data_arr_b)    
    @join_tables.push(column_on_db_a)
    @join_tables.push(column_on_db_b)
    return self
  end

  def order(order, column_name)
    @sort_data.push(order)
    @sort_data.push(column_name)
    return self
  end

  def insert(table_name)
    if table_name.end_with?('.csv')
      @file_name = table_name
    else
      @file_name = table_name+'.csv'    
    end 
    @insert_new_row = true
    return self
  end

  def values(data)
    @insert_data = data
    return self
  end

  def update(table_name)
    if table_name.end_with?('.csv')
      @file_name = table_name
    else
      @file_name = table_name+'.csv'    
    end 
    return self
  end

  def set(data)
    @update_data = data
    return self
  end

  def delete
    @delete_data = true
    return self
  end

  def run
    data_array = CSV.parse(File.read(@file_name))
    @all_columns = data_array.shift   
    @data_table = create_table(@all_columns, data_array)
    
    if @select_cols.length == 0 || @select_cols[0] == '*'
      @select_cols = @all_columns
    end

    filter_query_response()

    if !@update_data.empty?
      update_table()
      update_file()
    end

    if @delete_data == true
      delete_data()
      update_file()
    end

    if @insert_new_row == true
      insert_data()
    end
    #COMMENT PRINT IF GOING TO USE CLI
      print @query_response
       
    return @query_response

  end 
end


#TESTCASES#

#SELECT#
=begin
request = MySqliteRequest.new
request = request.from('nba_player_data.csv')
request = request.select('name')
#request = request.order('ASC','name')
request.run
=end

#SELECT WHERE#
=begin
request = MySqliteRequest.new
request = request.from('nba_player_data.csv')
request = request.select('name')
request = request.where('college', 'University of Oklahoma')
request.run
=end

#SELECT MULTIPLE WHERE#
=begin
request = MySqliteRequest.new
request = request.from('nba_player_data.csv')
request = request.select('name')
request = request.where('college', 'Louisiana State University')
request = request.where('year_start', '1991')
request.run
=end

#INSERT#
=begin
request = MySqliteRequest.new
request = request.insert('nba_player_data.csv')
request = request.values('name' => 'Alaa Abdelnaby', 'year_start' => '1991', 'year_end' => '1995', 'position' => 'F-C', 'height' => '6-10', 'weight' => '240', 'birth_date' => "June 24, 1968", 'college' => 'Duke University')
request.run
=end

#UPDATE#
=begin
request = MySqliteRequest.new
request = request.update('nba_player_data.csv')
request = request.where('name', 'Ivan Dulin')
request = request.set(:name => 'Ivan Pupkov')
request.run
=end


#DELETE#
=begin
request = MySqliteRequest.new
request = request.delete
request = request.from('nba_player_data.csv')
request = request.where('name', 'Alaa Abdelnaby')
request.run
=end

#JOIN#
=begin
request = MySqliteRequest.new
request = request.from('nba_player_data.csv')
request = request.select(['name','year_start','favorite_color'])
request = request.join('name', 'nba_player_data1.csv', 'player')
request.run
=end
