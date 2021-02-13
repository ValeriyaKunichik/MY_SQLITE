require "readline"
require "./my_sqlite_request.rb"

class CLI
  def initialize
    @args_array
    @insert_hash = {} 
    @set_keys=[] 
    @set_values=[] 
    @insert_query
    @join_query
  end

  def parse_values(query)
    query_values = []
    @args_array.each_index do |index|
      if (@args_array[index] == query)
        index += 1    

        if @args_array[index-1]=='ON'
          query_values.push(@args_array[index]) 
          query_values.push(@args_array[index+2])
          return query_values
        end   

        if (@args_array[index].start_with?'(')           
          line=@args_array[index]
          line.gsub!('(', '')
          line.gsub!(')', '')
          @args_array[index]=line
          @args_array[index]
          query_values= @args_array[index].split(',')   
          return query_values
        end

        if (@args_array[index-1]=='SET'&& @args_array[index+1] == '=')           
          @set_keys.push(@args_array[index]) 
          @set_values.push(@args_array[index+2].gsub!(",", ""))  
          index+=3
          while @args_array[index]!='WHERE'
            @set_keys.push(@args_array[index]) 
            @set_values.push(@args_array[index+2])  
            index+=3   
          end    
            return nil
        end

        if @args_array[index].include?(',')
          args=[] 
          args= @args_array[index].split(',')        
        else
          args= @args_array[index]
        end

        query_values.push(args)

        if (@args_array[index+1] == '=' && @args_array[index-1]=='WHERE')    
           query_values.push(@args_array[index+2])  
           return query_values
        end  

        return query_values             
      end
    end    
  end

  def parse_query(get_args)
    @args_array = get_args.split()
    @args_array.each do |query|
      case query

      when 'SELECT'         
        select_query = parse_values('SELECT')
        @request = @request.select(*select_query) 

      when 'INSERT'        
        if @args_array[1] != 'INTO'
          STDERR.puts "Correct form: INSERT INTO"
        else
          @args_array.slice!(1)
          @insert_query = parse_values('INSERT')
          @request = @request.insert(*@insert_query)
        end

      when 'VALUES'      
        values_query = parse_values('VALUES')
        cols= get_all_columns(*@insert_query)
        values_query.each_index do |index|
          @insert_hash["#{cols[index]}"] = values_query[index]
        end
        @request = @request.values(@insert_hash)

      when 'UPDATE'      
        update_query = parse_values('UPDATE')
        @request = @request.update(*update_query)

      when 'SET'             
        set_query = parse_values('SET')
        set_query_hash = {}
        index=0
        size=@set_keys.length()
        while index<size
          set_query_hash[:"#{@set_keys[index]}"] = @set_values[index].gsub!("'","")
          index+=1
        end
        @request = @request.set(set_query_hash)

      when 'DELETE'       
        delete_query = parse_values('DELETE')
        @request = @request.delete

      when 'FROM'          
        from_query = parse_values('FROM')      
        @request = @request.from(*from_query) 

      when 'WHERE'           
        where_query = parse_values('WHERE')
        column_name=where_query[0]       
        criteria= where_query[1].gsub("'", "")        
        @request = @request.where(column_name,criteria)

      when 'JOIN'
        @join_query = parse_values('JOIN')

      when 'ON'
        on_query = parse_values('ON')
        @request = @request.join(on_query[0],@join_query[0],on_query[1])
      end

    end
  end

  def run
    puts 'My_SQLite by Valerie Kunichik'
    get_args = Readline.readline("my_sqlite_cli> ", true)    
    while get_args != 'quit'
      @request = MySqliteRequest.new
      parse_query(get_args)    
      puts @request.run
      get_args = Readline.readline("my_sqlite_cli> ", true)
    end
  end
end

cli = CLI.new.run



#TESTCASES
=begin
SELECT * FROM students
SELECT name,email FROM students WHERE name = 'Mila'
INSERT INTO students VALUES (John,john@johndoe.com,A,https://blog.johndoe.com)
UPDATE students SET email = 'jane@janedoe.com', blog = 'https://blog.janedoe.com' WHERE name = 'Mila'
DELETE FROM students WHERE name = 'John'
SELECT name,lastname,mark FROM students JOIN students1 ON name=name
=end
