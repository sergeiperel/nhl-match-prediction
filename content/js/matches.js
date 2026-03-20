let activeDate = null
let allMatches = []


function getMatchStatus(iso){

    const start = new Date(iso)
    const now = new Date()

    const diffMin = (now - start) / 60000

    if(diffMin >= 0 && diffMin <= 180){
        return "live"
    }

    if(diffMin < 0){
        return "upcoming"
    }

    return "finished"

}

function formatDateTime(iso){

    const d = new Date(iso)

    const datePart =
        d.toLocaleDateString("ru-RU", {
            weekday: "short",
            day: "numeric",
            month: "short"
        })

    const timePart =
        d.toLocaleTimeString("ru-RU", {
            hour: "2-digit",
            minute: "2-digit",
            hour12: false
        })

    return `${datePart} • ${timePart}`
}

function formatDate(date){
    const y = date.getFullYear()
    const m = String(date.getMonth()+1).padStart(2,"0")
    const d = String(date.getDate()).padStart(2,"0")
    return `${y}-${m}-${d}`
}


function createDateButtons(){

    const container = document.getElementById("dateButtons")

    const today = new Date()

    const labels = ["Today","Tomorrow"]

    const days = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"]

    const buttons = []

    buttons.push({
        label:"Today",
        date:formatDate(today)
    })

    const tomorrow = new Date(today)
    tomorrow.setDate(tomorrow.getDate()+1)

    buttons.push({
        label:"Tomorrow",
        date:formatDate(tomorrow)
    })



    for(let i=2;i<7;i++){

        const d = new Date(today)
        d.setDate(today.getDate()+i)

        buttons.push({
            label:days[d.getDay()],
            date:formatDate(d)
        })

    }

    buttons.forEach((btn,i)=>{

        const b = document.createElement("button")

        b.textContent = btn.label

        b.className =
        "date-btn bg-slate-800 hover:bg-slate-700 px-4 py-2 rounded-lg"

        if(i===0){
            b.classList.add("bg-blue-600")
            activeDate = btn.date
        }

        b.onclick = ()=>{

            document.getElementById("dateFilter").value = ""

            document.querySelectorAll(".date-btn").forEach(x=>{
                x.classList.remove("bg-blue-600")
                x.classList.add("bg-slate-800")
            })

            b.classList.remove("bg-slate-800")
            b.classList.add("bg-blue-600")

            activeDate = btn.date

            renderMatches()
        }

        container.appendChild(b)

    })

}

function isSameLocalDate(iso, yyyyMmDd){

    const d = new Date(iso)

    const local =
        d.getFullYear() + "-" +
        String(d.getMonth()+1).padStart(2,"0") + "-" +
        String(d.getDate()).padStart(2,"0")

    return local === yyyyMmDd
}

function renderMatches(){

    const container = document.getElementById("matches")

    container.innerHTML = ""

    let matches = allMatches

    const calendarDate = document.getElementById("dateFilter").value

    if(calendarDate){
        matches = matches.filter(m =>
            isSameLocalDate(m.game_date, calendarDate)
        )
    }
    else if(activeDate){
        matches = matches.filter(m =>
            isSameLocalDate(m.game_date, activeDate)
        )
    }

    matches.sort((a,b)=>{

        const statusA = getMatchStatus(a.game_date)
        const statusB = getMatchStatus(b.game_date)

        if(statusA === "live" && statusB !== "live") return -1
        if(statusB === "live" && statusA !== "live") return 1

        return new Date(a.game_date) - new Date(b.game_date)

    })

    let currentDay = null

    matches.forEach(match=>{

        const matchDate = new Date(match.game_date)
        const dayKey = formatDate(matchDate)

        // --- Заголовок дня ---
        if(dayKey !== currentDay){

            currentDay = dayKey

            const dayHeader = document.createElement("div")

            dayHeader.className =
            "text-xl font-bold text-slate-300 mt-8 mb-2 border-b border-slate-700 pb-2"

            dayHeader.textContent =
                matchDate.toLocaleDateString("ru-RU", {
                    weekday: "long",
                    day: "numeric",
                    month: "long"
                })

            container.appendChild(dayHeader)

        }

        const status = getMatchStatus(match.game_date)

        const statusBadge =
        status === "live"
            ? `<span class="ml-2 px-2 py-1 text-xs bg-red-600 rounded animate-pulse">LIVE</span>`
        : status === "finished"
            ? `<span class="ml-2 px-2 py-1 text-xs bg-slate-600 rounded">FINAL</span>`
        : ""

        const vsText = status === "upcoming" ? "vs" : ""

        const score =
            status !== "upcoming" &&
            match.home_score != null &&
            match.away_score != null
                ? `<div class="text-3xl font-bold text-center my-2">
                    ${match.home_score} : ${match.away_score}
                </div>`
                : ""


        const div = document.createElement("div")

        div.className =
        "border border-slate-700 rounded-xl p-5 bg-slate-900 shadow-lg"

        if(status === "live"){
            div.classList.add("ring-2","ring-red-500")
        }

        const homeProb = Math.round((match.prediction_prob ?? 0.5) * 100)
        const awayProb = 100 - homeProb

        const homeColor = homeProb > awayProb ? "text-green-400" : "text-white"
        const awayColor = awayProb > homeProb ? "text-red-400" : "text-white"

        let label = "Toss-up"

        if(homeProb > 70 || awayProb > 70) label = "Strong favorite"
        else if(homeProb > 60 || awayProb > 60) label = "Favorite"
        else if(homeProb > 55 || awayProb > 55) label = "Lean"

        div.innerHTML = `

    <div class="flex justify-between items-center mb-4">

    <div class="flex items-center gap-3">
    <img src="${match.home_team_logo}" class="w-16 h-16 rounded-full bg-white p-0.5 object-contain">
    <div class="text-xl font-semibold ${homeColor}">
    ${match.home_team_abbr}
    </div>
    </div>

    <div class="text-xl font-semibold text-slate-400">
    ${vsText} ${statusBadge}
    </div>

    ${score}

    <div class="flex items-center gap-3">
    <div class="text-xl font-semibold ${awayColor}">
    ${match.away_team_abbr}
    </div>
    <img src="${match.away_team_logo}" class="w-16 h-16 rounded-full bg-white p-0.5 object-contain">
    </div>

    </div>

    <div class="flex justify-center items-center gap-4 my-4">
    <span class="text-5xl font-bold text-green-400">${homeProb}%</span>
    <span class="text-3xl text-slate-400">—</span>
    <span class="text-5xl font-bold text-red-400">${awayProb}%</span>
    </div>

    <div class="w-full h-6 rounded-full bg-slate-700 relative overflow-hidden mb-3">

    <div class="h-6 absolute left-0 top-0 transition-all duration-700"
    style="width:${homeProb}%; background:#10b981"></div>

    <div class="h-6 absolute right-0 top-0 transition-all duration-700"
    style="width:${awayProb}%; background:#ef4444"></div>

    </div>

    <div class="text-center text-base text-slate-300 mb-2">
    ${label}
    </div>

    <div class="text-center text-base text-slate-400">
    ${formatDateTime(match.game_date)}
    </div>

    `

        container.appendChild(div)

    })



}

async function loadMatches() {

    try {
        const response = await fetch("predict_upcoming")

        if (!response.ok) {
            throw new Error("API error")
        }

        const data = await response.json()

        if (!Array.isArray(data)) {
            console.warn("Unexpected API response:", data)
            allMatches = []
        } else {
            allMatches = data
        }

        renderMatches()

    } catch (e) {
        console.error("Failed to load matches:", e)
    }

}

document.getElementById("dateFilter").addEventListener("change",(e)=>{

    activeDate = null

    document.querySelectorAll(".date-btn").forEach(x=>{
        x.classList.remove("bg-blue-600")
        x.classList.add("bg-slate-800")
    })

    renderMatches()

})

createDateButtons()

loadMatches()
