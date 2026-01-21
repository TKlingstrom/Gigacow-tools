#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    https://shiny.posit.co/
#

library(shiny)
library(DBI)
library(odbc)
library(dplyr)
library(dbplyr)
library(ggplot2)
library(lubridate)
library(DT)
library(shinyjs)


#Connecting to the database using the R user credentions.
con <- dbConnect(odbc(),
                 Driver = "SQL Server",
                 Server = "gigacow.db.slu.se",
                 Database = "Gigacow"
)

#Shows the available tables in the schema.
odbcListObjects(con, catalog="Gigacow", schema="fact")
ui <- fluidPage(
  shinyjs::useShinyjs(),
  titlePanel("Gas sensor dashboard (CH4)"),
  sidebarLayout(
    sidebarPanel(
      h4("Farms"),
      fluidRow(
        column(6, actionButton("farms_all", "All")),
        column(6, actionButton("farms_none", "None"))
      ),
      checkboxGroupInput("places", NULL, choices = NULL),
      
      tags$hr(),
      
      h4("Gas sensors"),
      fluidRow(
        column(6, actionButton("sensors_all", "All")),
        column(6, actionButton("sensors_none", "None"))
      ),
      checkboxGroupInput("sensors", NULL, choices = NULL),
      
      tags$hr(),
      
      numericInput("channel", "Channel", value = 99),
      actionButton("refresh", "Refresh")
    ),
    mainPanel(
      plotOutput("plot_ch4", height = 420),
      DTOutput("table_data")
    )
  )
)

server <- function(input, output, session) {
  
  farm_choices <- reactive({
    con %>%
      tbl(in_catalog("Gigacow", "fact", "Gas")) %>%
      distinct(PlaceName) %>%
      arrange(PlaceName) %>%
      collect() %>%
      pull(PlaceName)
  })
  
  observe({
    farms <- farm_choices()
    updateCheckboxGroupInput(session, "places", choices = farms, selected = farms)
  })
  
  sensor_choices <- reactive({
    req(input$places)
    con %>%
      tbl(in_catalog("Gigacow", "fact", "Gas")) %>%
      filter(PlaceName %in% input$places) %>%
      distinct(GasSensorNumber) %>%
      arrange(GasSensorNumber) %>%
      collect() %>%
      pull(GasSensorNumber)
  })
  
  observeEvent(input$places, {
    sens <- sensor_choices()
    updateCheckboxGroupInput(session, "sensors", choices = sens, selected = sens)
  }, ignoreInit = TRUE)
  
  observeEvent(input$farms_all, {
    farms <- farm_choices()
    updateCheckboxGroupInput(session, "places", selected = farms)
  })
  
  observeEvent(input$farms_none, {
    updateCheckboxGroupInput(session, "places", selected = character(0))
  })
  
  observeEvent(input$sensors_all, {
    sens <- sensor_choices()
    updateCheckboxGroupInput(session, "sensors", selected = sens)
  })
  
  observeEvent(input$sensors_none, {
    updateCheckboxGroupInput(session, "sensors", selected = character(0))
  })
  
  data_gas <- eventReactive(input$refresh, {
    
    req(input$places, input$sensors, input$channel)
    
    start_date <- format(Sys.Date() - 30, "%Y-%m-%d")
    
    df <- con %>%
      tbl(in_catalog("Gigacow", "fact", "Gas")) %>%
      filter(
        PlaceName %in% input$places,
        GasSensorNumber %in% input$sensors,
        Channel == input$channel,
        MeasurementDate >= start_date
      ) %>%
      collect()
    
    # Parse date and time
    df$MeasurementDate <- ymd(as.character(df$MeasurementDate))
    df$MeasurementTime <- trimws(as.character(df$MeasurementTime))
    
    # Aggregate to daily mean CH4 in two windows
    df2 <- df %>%
      mutate(
        window = case_when(
          MeasurementTime >= "11:00:00" & MeasurementTime <= "11:30:00" ~ "11:00-11:30",
          MeasurementTime >= "23:00:00" & MeasurementTime <= "23:30:00" ~ "23:00-23:30",
          TRUE ~ NA_character_
        )
      ) %>%
      filter(!is.na(MeasurementDate), !is.na(window)) %>%
      group_by(PlaceName, GasSensorNumber, MeasurementDate, window) %>%
      summarise(CH4 = mean(CH4, na.rm = TRUE), .groups = "drop") %>%
      mutate(
        PlotTime = as.POSIXct(MeasurementDate) +
          ifelse(window == "11:00-11:30", 11 * 3600 + 15 * 60, 23 * 3600 + 15 * 60)
      ) %>%
      arrange(PlaceName, GasSensorNumber, PlotTime)
    
    df2
  }, ignoreInit = FALSE)
  
  output$plot_ch4 <- renderPlot({
    df <- data_gas()
    validate(need(nrow(df) > 0, "No data to plot for selected farms/sensors/time windows."))
    
    ggplot(
      df,
      aes(
        x = PlotTime,
        y = CH4,
        color = as.factor(GasSensorNumber),
        group = interaction(GasSensorNumber, window)
      )
    ) +
      geom_line() +
      geom_point(size = 2) +
      facet_wrap(~ PlaceName, scales = "free_y") +
      labs(
        x = "Time (window midpoint)",
        y = "Mean CH4",
        color = "GasSensor",
        title = paste("Channel", input$channel,
                      "- Daily means (11:00–11:30 & 23:00–23:30), last 30 days")
      )
  })
  
  output$table_data <- renderDT({
    datatable(
      data_gas(),
      options = list(pageLength = 25, autoWidth = TRUE),
      rownames = FALSE
    )
  })
  
  session$onSessionEnded(function() {
    try(dbDisconnect(con), silent = TRUE)
  })
}
shinyApp(ui, server)