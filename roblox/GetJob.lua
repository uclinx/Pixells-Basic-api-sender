local HttpService = game:GetService("HttpService")
local ReplicatedStorage = game:GetService("ReplicatedStorage")

local REMOTE_EVENT_NAME = "JobUpdate"
local POLL_INTERVAL = 2 -- تعليق: نرسل طلب كل ثانيتين فقط
local API_URL = "https://pixells-basic.onrender.com/job/latest"

local remoteEvent = ReplicatedStorage:FindFirstChild(REMOTE_EVENT_NAME)
if not remoteEvent then
    remoteEvent = Instance.new("RemoteEvent")
    remoteEvent.Name = REMOTE_EVENT_NAME
    remoteEvent.Parent = ReplicatedStorage
end

local lastJobId = nil
local lastTimestamp = 0

local function fetchLatestJob()
    local success, result = pcall(function()
        -- تعليق: نرسل طلب GET للحصول على آخر وظيفة من Render
        local response = HttpService:GetAsync(API_URL)
        return HttpService:JSONDecode(response)
    end)

    if not success then
        warn("[JobPoller] HTTP error:", result)
        return
    end

    local jobData = result.job
    if not jobData then
        return -- تعليق: لا يوجد بيانات حالياً
    end

    if jobData.job_id == lastJobId and jobData.timestamp == lastTimestamp then
        return -- تعليق: إذا الوظيفة لم تتغير نتجاهلها
    end

    lastJobId = jobData.job_id
    lastTimestamp = jobData.timestamp or 0

    -- تعليق: نسجل الوظيفة الجديدة ونبلغ اللاعبين
    print("[JobPoller] New job received:", jobData.job_id, jobData.name, jobData.money, jobData.players)
    remoteEvent:FireAllClients(jobData)
end

while task.wait(POLL_INTERVAL) do
    fetchLatestJob()
end
