dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')
local item_type = nil
local item_name = nil
local item_value = nil

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false

local queued_pages = {}

local discovered = {}

local bad_items = {}

if not urlparse or not http then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local ids = {}

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

get_item = function(url)
  local match = string.match(url, "^https?://www%.xtube%.com/video%-watch/%-([0-9]+)$")
  local type_ = "v"
  if not match then
    match = string.match(url, "^https?://www%.xtube%.com/profile/%-%-([0-9]+)$")
    type_ = "p"
  end
  if not match then
    match = string.match(url, "^https?://www%.xtube%.com/search/video/([^/%?&]+)$")
    type_ = "s"
  end
  if not match then
    match = string.match(url, "^https?://cdn([^/]*%.xtube%.com/.+)$")
    if match and (
      string.match(match, "validfrom=")
      or string.match(match, "ttl=")
      or string.match(match, "%.m3u8")
      or string.match(match, "%.ts")
    ) then
      match = nil
    end
    type_ = "cdn"
  end
  if match and type_ then
    return type_, match
  end
end

set_new_item = function(url)
  local type_, match = get_item(url)
  if match and not ids[match] then
    abortgrab = false
    ids[match] = true
    item_value = match
    item_type = type_
    item_name = type_ .. ":" .. match
    io.stdout:write("Archiving item " .. item_name .. ".\n")
    io.stdout:flush()
  end
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

allowed = function(url, parenturl)
  if item_type == "cdn" then
    return true
  end

  if string.match(urlparse.unescape(url), "[<>\\%*%$%^%[%]%(%){}]") then
    return false
  end

  local tested = {}
  for s in string.gmatch(url, "([^/]+)") do
    if not tested[s] then
      tested[s] = 0
    end
    if tested[s] == 6 then
      return false
    end
    tested[s] = tested[s] + 1
  end

  for newurl in string.gmatch(url, "([^;]+)") do
    local type_, value = get_item(newurl)
    if value and type_ == "s" or type_ == "cdn" then
      local new_item = type_ .. ":" .. value
      if not discovered[new_item] then
        discovered[new_item] = true
      end
      return false
    end
  end

  if string.match(url, "^https?://cdn") then
    if (string.match(url, "validfrom=") or string.match(url, "ttl="))
      and string.match(url, "%.webm") then
      return false
    end
    return true
  end

  if item_type == "s" then
    local match = string.match("/search/video/([/%?&]+)")
    if match and ids[match] then
      return true
    end
    return false
  end

  if item_type == "p" then
    local match = string.match(url, "^https?://www%.xtube%.com/gallery/.-%-([0-9]+)")
    if not match then
      match = string.match(url, "^https?://www%.xtube%.com/gallerydetail/paid/([0-9a-zA-Z]+)")
    end
    if not match then
       match = string.match(url, "^https?://www%.xtube%.com/community/blog/detail/.-%-([0-9]+)")
    end
    if match then
      ids[match] = true
    end
  end

  for s in string.gmatch(url, "([0-9]+)") do
    if ids[s] then
      return true
    end
  end

  for s in string.gmatch(url, "([0-9a-zA-Z]+)") do
    if ids[s] then
      return true
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  local function check(urla, headers)
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.match(url, "^(.-)%.?$")
    url_ = string.gsub(url_, "&amp;", "&")
    url_ = string.match(url_, "^(.-)%s*$")
    url_ = string.match(url_, "^(.-)%??$")
    url_ = string.match(url_, "^(.-)&?$")
    url_ = string.match(url_, "^(.-)/?$")
    if not string.match(url_, "^https?://[^/]+/.") then
      return nil
    end
    if (downloaded[url_] ~= true and addedtolist[url_] ~= true)
      and allowed(url_, origurl) then
      if headers ~= nil and not string.match(url_, "^https?://cdn") then
print('with xml', url_)
        table.insert(urls, { url=url_, headers=headers })
      else
        table.insert(urls, { url=url_ })
      end
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl, headers)
    if string.match(newurl, "^#") then
      return nil
    end
    if string.match(newurl, "\\[uU]002[fF]") then
      return checknewurl(string.gsub(newurl, "\\[uU]002[fF]", "/"), headers)
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"), headers)
    elseif string.match(newurl, "^https?://") then
      check(newurl, headers)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""), headers)
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""), headers)
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl), headers)
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl), headers)
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl), headers)
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"), headers)
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl), headers)
    end
  end

  local function checknewshorturl(newurl, headers)
    if string.match(newurl, "^#") then
      return nil
    end
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl), headers)
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^%${")) then
      check(urlparse.absolute(url, newurl), headers)
    else
      checknewurl(newurl, headers)
    end
  end

  if allowed(url, nil) and status_code == 200 and item_type ~= "cdn"
    and (
      not string.match(url, "^https?://cdn")
      or string.match(url, "%.m3u8")
    ) then
    html = read_file(file)
    --[[if string.match(url, "^https?://www%.xtube%.com/video%-watch/")
      and not string.match(html, "%.m3u8") then
      return urls
    end]]
    if string.match(html, "^{") then
      local data = JSON:decode(html)
      if data["status"] ~= "OK" then
        error("Response not OK.")
      end
      local newurl = string.match(url, "^(https?://.+)/[0-9]+$")
      if not newurl then
        newurl = url
      end
      queued_pages[string.match(url, "^https?://[^/]+(.-)$")] = true
      for i=1,data["pageCount"] do
        check(newurl .. "/" .. tostring(i), {["X-Requested-With"]="XMLHttpRequest"})
      end
      html = data["html"]
    end
    if string.match(url, "%.m3u8") then
      for s in string.gmatch(html, "([^\n]+)") do
        checknewshorturl(s)
      end
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '%s+data%-href="([^"]+)"') do
      checknewshorturl(newurl, {["X-Requested-With"]="XMLHttpRequest"})
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "%(([^%)]+)%)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  set_new_item(url["url"])
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if downloaded[newloc] or addedtolist[newloc]
      or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.ABORT
  end

  if status_code == 471 and string.match(url["url"], "%.mp4%?") then
    return wget.actions.EXIT
  end

  if status_code == 0 or (status_code ~= 200 and status_code ~= 404) then
    --or (status_code >= 400 and status_code ~= 404) then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 3
    if tries >= maxtries then
      io.stdout:write("I give up...\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.ABORT
    else
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local items = nil
  for item, _ in pairs(discovered) do
    print('found item', item)
    if items == nil then
      items = item
    else
      items = items .. "\0" .. item
    end
  end
  if items ~= nil then
    local tries = 0
    while tries < 10 do
      local body, code, headers, status = http.request(
        "http://blackbird-amqp.meo.ws:23038/xtube-2e6pvmjphdaz7nb/",
        items
      )
      if code == 200 or code == 409 then
        break
      end
      io.stdout:write("Could not queue items.\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == 10 then
      abort_item()
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab then
    abort_item()
    return wget.exits.IO_FAIL
  end
  return exit_status
end

