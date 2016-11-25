module Pop2ExchangeExporter

"""
does the given line delimit a log entry
"""
is_log_entry_delimiter(line) = all(x->x == '-', SubString(line, 1, length(line)-2)) && length(line) > 3

"""
a parse log entry
"""
immutable LogEntry
    timestamp::DateTime
    error::Bool
    mail_count::Int
    msg::String
end

"""
the date format used in the log file
"""
const log_date_format = Dates.DateFormat("d.m.y H:M:S")
const db_date_format = Dates.DateFormat("YYYY-mm-dd HH:MM:SS")

"""
parse a log entry given as a string into a LogEntry object
"""
function parse_log_entry{T <: Union{DateTime, Void}}(raw_entry::String, timestamp_hint::T = nothing)
    # get timestamp
    timestamp = let m = match(r"check at( local time)*: ([^\n\r]+)"i, raw_entry)
        # if not timestamp was found use the one from the last entry
        if m == nothing
            @assert timestamp_hint != nothing "log entry had no timestamp and no previous entry was given to deduce one"
            timestamp_hint
        else
            DateTime(m[2], log_date_format)
        end
    end::DateTime

    # did an error occur in this entry
    error = ismatch(r"Error", raw_entry)

    # how many messages were processed
    message_count = sum(map(m -> parse(Int, m[1]), eachmatch(r"[a-zA-Z]+ -> ([0-9]+) new messages", raw_entry)))

    #
    # create LogEntry object
    #
    LogEntry(timestamp, error, message_count, raw_entry)
end

function time_difference_in_days(t1, t2)
    Dates.Day(trunc(t1, Dates.Day)-trunc(t2, Dates.Day))
end

"""
extract the body from a production function and wrap it inside an anonymous
function without arguments, a producer.

@production function production(production_count)
    for i=1:production_count
        produce(i)
    end
end

will be transformed into

function production(production_count)
    Task(() -> begin
        for i=1:production_count
            produce(i)
        end
    end)
end
"""
macro production(producer_expr)
    @assert producer_expr.head == :function "producer must be a function"
    :(function $(producer_expr.args[1].args[1])($(producer_expr.args[1].args[2:end]...))
        Task(() -> $(producer_expr.args[2]))
    end)
end

"""
reads log file line by line and group log entries, whenever a log entry
has been parsed it is given to the cosumer by a call to produce
"""
@production function raw_log_entry_production(file::String, offset=0)
    # all entries are stored in this array
    local entry=[]
    # check offset
    if filesize(file)<offset
        warn("Log file was smaller then the given offset. Reading from the beginning.")
        offset=0
    end
    # read log file and group entries
    open(file) do s
        pos=0
		# seek to the beginning of the unparsed part of the log file
		if offset != 0
			seek(s, offset)
		end
		# read in log file line by line
        while !eof(s)
            let line = readline(s)
                if is_log_entry_delimiter(line)
                    if length(entry) == 0
                        continue
                     end
					produce(strip(join(entry))) # produce entry
                    entry=[]
                else 
                    push!(entry, line) # current line belongs to the current entry
                    pos=position(s)
                end
            end
        end
        # process remaining data
        #if length(entry) != 0
        #    produce(strip(join(entry)))
        #end
		pos
    end
end

"""
read log file and parse entries into LogEntry objects
"""
@production function log_entry_production(args...)
    # obtain producer responsible for reading the log file and grouping by entries
    production = raw_log_entry_production(args...)
	schedule(production)
	yield()
	if !istaskdone(production)
		last_entry = parse_log_entry(first(production))
		produce(last_entry)
		for entry in production
			last_entry = parse_log_entry(entry, last_entry.timestamp)
			produce(last_entry)
		end
	end
	offset = production.result
end

using SQLite

"""
open the log database and insert all entries from the log file that
have not been parsed yet
"""
function update_database(log_file, db="pop2exchange.sqlite")
    log_file = abspath(log_file) # make paths absolute
    tic() # start timing
    info("started database update")
    # open the database
    if isa(db,  SQLite.DB)
        info("using database `$(db.file)`")
    else
        info("open database `$(db)`")
        db = SQLite.DB(db)
    end
    # check wether the log table exists
    SQLite.execute!(db, """CREATE TABLE IF NOT EXISTS "logs"
	  (
		 "id"          INTEGER PRIMARY KEY NOT NULL UNIQUE,
		 "timestamp"   DATETIME NOT NULL,
         "error"       BOOLEAN NOT NULL,
		 "mail_count" INTEGER NOT NULL,
		 "message"     TEXT NOT NULL
	  )
	""")
	SQLite.execute!(db, """CREATE TABLE IF NOT EXISTS "metadata"
	  (
		 "key"   TEXT PRIMARY KEY NOT NULL UNIQUE,
		 "value" NOT NULL
	  ) 
	""")
	# get log file offset (marks the last byte of the log file that 
	#  was already parsed and imported into the database on the last run)
	local offset
	let query=SQLite.query(db, "SELECT value FROM metadata WHERE key = 'offset'")
		if size(query, 1)==0
			SQLite.execute!(db, """INSERT INTO "metadata" ("key","value") VALUES ("offset", 0)""")
			offset = 0
		else
			offset = get(query[1, 1])
		end
	end
    info("reading log file `$(log_file)` ($(filesize(log_file)) bytes) from offset $(offset)")
    # prepare sql statement to insert new log entries into the database
	insert=SQLite.Stmt(db, """INSERT INTO logs (timestamp, error, mail_count, message) VALUES (?, ?, ?, ?);""")
	# begin transaction (improves speed) and ensures a correct 
	SQLite.execute!(db, "BEGIN TRANSACTION")
	# read on log file
	producer = log_entry_production(log_file, offset)
	let length = 0
		for log_entry in producer
			SQLite.bind!(insert, 1, Dates.format(log_entry.timestamp, db_date_format))
            SQLite.bind!(insert, 2, Int(log_entry.error))
			SQLite.bind!(insert, 3, log_entry.mail_count)
			SQLite.bind!(insert, 4, log_entry.msg)
			SQLite.execute!(insert)
			length +=1
		end
		info("successfully inserted $(length) new entries")
	end
	# write log file offset
	offset = producer.result
	SQLite.execute!(db, """UPDATE "metadata" SET "value" = $(offset) WHERE "key" = "offset" """)
	# end transaction
	SQLite.execute!(db, "END TRANSACTION")
    # print some information
    info("total number of recieved mails: ", recieved_mails_total(db))
    try info("last error: ", time_difference_in_days(now(), last_error(db)), " days ago") end
    try info("last check: ", round(Int, (now()-last_check(db)).value/1000/60), " minutes ago") end
    info("elapsed time: ", round(toq(), 2), " seconds")
    info("finished database update")
    println()
end

function recieved_mails_total(db)
    q = SQLite.query(db, "SELECT SUM(mail_count) FROM logs")[1, 1]
    isnull(q) ? 0 : get(q)
end

function last_error(db)
    q=SQLite.query(db, "SELECT timestamp FROM logs WHERE error=1 ORDER BY timestamp DESC LIMIT 0,1")
    size(q, 1) > 0 ? DateTime(get(q[1, 1]), db_date_format) : "N/A"
end

function last_check(db)
    q=SQLite.query(db, "SELECT timestamp FROM logs ORDER BY timestamp DESC LIMIT 0,1")
    size(q, 1) > 0 ? DateTime(get(q[1, 1]), db_date_format) : "N/A"
end

using HttpServer

function daemon(db_file = "pop2exchange.sqlite", log_file="/media/logs/Pop2Exchange.log")
    info("open database `$(db_file)`")
    db = SQLite.DB(db_file)

    info("starting parser")
    @async while true
        try
            entries = update_database(log_file, db)
        catch e
            warn("error during update: ", e)
        end
        sleep(60)
    end

    http = HttpHandler() do req::Request, res::Response
        # build a response
        resp_text = """
        # TYPE pop2exchange_recieved_mails counter
        pop2exchange_recieved_mail_total $(recieved_mails_total(db))
        # TYPE pop2exchange_last_error gauge
        pop2exchange_last_error $(last_error(db)=="N/A" ? -1 : round(Int, (now()-last_error(db)).value/1000))
        # TYPE pop2exchange_last_check gauge
        pop2exchange_last_check $(last_check(db)=="N/A" ? -1 : round(Int, (now()-last_check(db)).value/1000))
        """
        resp = Response(200, Dict{AbstractString, AbstractString}("Content-Type" => "text/plain"), resp_text)
        Response( ismatch(r"^/metrics(/)*",req.resource) ? resp : 404 )
    end

    server = Server( http )
    run( server, 8000 )
end

end # end module
