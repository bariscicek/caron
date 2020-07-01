local cnt = 0
local err = 0
while ((redis.call("JSON.ARRLEN", "PROGRAM_LIST") ~= 0) and (cnt < PROGRAM_BATCH)) do
  local curval = redis.call("JSON.ARRPOP", "PROGRAM_LIST")
  redis.call("JSON.SET", "curval", '.', curval)
  local jqueue = redis.pcall("JSON.GET", "curval", ".queue")
  if jqueue["err"] then 
    jqueue = "PROGRAM_DEF_QUEUE"
    redis.call("JSON.SET", "curval", ".queue", '"PROGRAM_DEF_QUEUE"', 'NX') end

  local pushCmd = "PROGRAM_Q_LIFO" .. "PUSH"

  local jobId = redis.pcall("JSON.GET", "curval", ".jobId")
  if jobId["err"] then jobId = redis.call("INCR", "bull:" .. jqueue .. ":id") end

  local jattempts = redis.pcall("JSON.GET", "curval", ".attempts")
  if jattempts["err"] then jattempts = nil else redis.pcall("JSON.DEL", "curval", ".attempts") end

  local jdelay = redis.pcall("JSON.GET", "curval", ".delay")
  if jdelay["err"] then jdelay = nil else redis.pcall("JSON.DEL", "curval", ".delay") end

  if (type(jattempts) ~= "number") or (jattempts <= 0) then jattempts = PROGRAM_DEF_ATTEMPTS end
  if (type(jdelay) ~= "number") or (jdelay < 0) then jdelay = 0 end

  local jopt0 = redis.pcall("JSON.GET", "curval", ".removeOnComplete")
  if jopt0["err"] then jopt0 = true else redis.pcall("JSON.DEL", "curval", ".removeOnComplete") end
  
  if (type(jopt0) ~= "boolean") then jopt0 = true end
  local jopts = { removeOnComplete = jopt0, delay = jdelay, attempts = jattempts }

  local jobIdKey = "bull:" .. jqueue .. ":" .. jobId
  
  curval = redis.call("JSON.GET", "curval", '.')
  if redis.call("EXISTS", jobIdKey) == 0 then
    redis.call("HMSET", jobIdKey, "data", curval, "opts", cjson.encode(jopts), "progress", 0, "delay", jdelay, "timestamp", ARGV[2], "attempts", jattempts, "attemptsMade", 0, "stacktrace", "[]", "returnvalue", "null")
    if jdelay > 0 then
      local timestamp = (tonumber(ARGV[2]) + jdelay) * 0x1000 + bit.band(jobId, 0xfff)
      redis.call("ZADD", "bull:" .. jqueue .. ":delayed", timestamp, jobId)
      redis.call("PUBLISH", "bull:" .. jqueue .. ":delayed", (timestamp / 0x1000))
    else
      if redis.call("EXISTS", "bull:" .. jqueue .. ":meta-paused") ~= 1 then
        redis.call(pushCmd, "bull:" .. jqueue .. ":wait", jobId)
      else
        redis.call(pushCmd, "bull:" .. jqueue .. ":paused", jobId)
      end
      redis.call("PUBLISH", "bull:" .. jqueue .. ":waiting@null", jobId)
    end
  end

  redis.call("JSON.DEL", "curval", ".")
end
return {err, cnt}
