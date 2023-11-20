library(arrow)
library(tidyverse)
library(SPHSUgraphs)
library(furrr)

plan(multisession, workers = 5)

baseline_main <- open_dataset(file.path("out_data", "reform_main", "combined_data"))
runs <- baseline_main |> 
  select(run) |> 
  unique() |> 
  collect()


summarise_arrow <- function(data_name) {
  
  dataset <- open_dataset(file.path("out_data", data_name, "combined_data"))
  
  dataset |> 
  filter(dag >= 18, dag <= 65) |>
  mutate(ghq_case = dhm_ghq,
         employed = les_c4 == "EmployedOrSelfEmployed"
         ) |> 
  group_by(run, time) |> 
  summarise(
    across(
      c(
        dhm,
        employed,
        ghq_case,
        equivalisedDisposableIncomeYearly,
        atRiskOfPoverty
      ),
      list(mean = mean)
    ),
    across(
      c(ghq_case, atRiskOfPoverty, employed),
      list(n = sum)
    ),
    N = n(),
    .groups = "drop"
  ) |> 
  mutate(scenario = data_name) |> 
  collect()
  
}

summarise_batch <- function(data_name) {
  
  labour_supply_levels <-  c("ZERO" = 0,
                             "TEN" = 10,
                             "TWENTY" = 20,
                             "THIRTY" = 30,
                             "FORTY" = 40)
  
  median_inc_batched <- runs |>
    mutate(data_out = future_map(run, \(run_n) {
      open_dataset(file.path(
        "out_data",
        data_name,
        "combined_data",
        paste0("run=", run_n)
      )) |>
        select(time, equivalisedDisposableIncomeYearly, dhm, labourSupplyWeekly) |>
        collect() |>
        mutate(labour_supply_numeric = labour_supply_levels[labourSupplyWeekly]) |> 
        group_by(time) |>
        summarise(
          across(
            c(dhm, equivalisedDisposableIncomeYearly, labour_supply_numeric),
            list(
              median = median,
              q_10 = ~ quantile(.x, 0.10),
              q_90 = ~ quantile(.x, 0.90),
              q_25 = ~ quantile(.x, 0.25),
              q_75 = ~ quantile(.x, 0.75)
            )
          ),
          across(
            c(labour_supply_numeric),
            list(mean = mean)
          ),
          .groups = "drop"
        ) |> 
        mutate(scenario = data_name)
    })) |> 
    unnest(data_out)
  
  
}

count_labour_cats <- function(data_name) {
  open_dataset(file.path("out_data", data_name, "combined_data")) |>
    count(run, time, labourSupplyWeekly) |>
    collect() |>
    pivot_wider(names_from = labourSupplyWeekly,
                values_from = n,
                names_glue = "n_labour_{labourSupplyWeekly}") |> 
    mutate(scenario = data_name)
  
  
}


# Use functions to do this double-reading for each scenario:

baseline_dat1 <- summarise_arrow("baseline_main")
baseline_dat2 <- summarise_batch("baseline_main")
baseline_dat3 <- count_labour_cats("baseline_main")

baseline_main_stats <-
  full_join(baseline_dat1, baseline_dat2, by = c("run", "time", "scenario")) |>
  full_join(baseline_dat3, by = c("run", "time", "scenario"))

reform_dat1 <- summarise_arrow("reform_main")
reform_dat2 <- summarise_batch("reform_main")
reform_dat3 <- count_labour_cats("reform_main")

reform_main_stats <-
  full_join(reform_dat1, reform_dat2, by = c("run", "time", "scenario")) |>
  full_join(reform_dat3, by = c("run", "time", "scenario"))

baseline_main_stats |> 
  bind_rows(reform_main_stats) |> 
  ggplot(aes(time, equivalisedDisposableIncomeYearly_median, colour = scenario, fill = scenario)) +
  geom_vline(aes(xintercept = 2019, linetype = "Covid reform\nimplementation"),
             colour = "red") +
  stat_summary(
    fun.data = median_hilow,
    geom = "ribbon",
    alpha = 0.5,
    colour = NA
  ) +
  stat_summary(fun = median, geom = "line") +
  stat_summary(fun = median, geom = "point") +
  scale_fill_manual(
    "Policy:",
    aesthetics = c("fill", "colour"),
    breaks = c("baseline_main", "reform_main"),
    labels = c("Baseline", "Covid policy"),
    values = sphsu_cols("University Blue", "Rust", names = FALSE)
  ) +
  scale_linetype("") +
  xlab("Year") +
  scale_y_continuous(
    "Median disposable income",
    labels = scales::dollar_format(prefix = "£")
  ) +
  labs(
    caption = paste(
      "Notes:",
      "1,000 simulation runs in each condition.",
      "Red line denotes reform implementation point",
      sep = "\n"
    )
  ) +
  theme_sphsu_light() +
  theme(legend.position = "bottom",
        plot.caption = element_text(hjust = 0))



# sensitivity -------------------------------------------------------------



baseline_dat_sens1 <- summarise_arrow("baseline_sens")
baseline_dat_sens2 <- summarise_batch("baseline_sens")
baseline_dat_sens3 <- count_labour_cats("baseline_sens")

reform_dat_sens1 <- summarise_arrow("reform_sens")
reform_dat_sens2 <- summarise_batch("reform_sens")
reform_dat_sens3 <- count_labour_cats("reform_sens")



baseline_sens_stats <-
  full_join(baseline_dat_sens1, baseline_dat_sens2, by = c("run", "time", "scenario")) |>
  full_join(baseline_dat_sens3, by = c("run", "time", "scenario"))

reform_sens_stats <-
  full_join(reform_dat_sens1, reform_dat_sens2, by = c("run", "time", "scenario")) |>
  full_join(reform_dat_sens3, by = c("run", "time", "scenario"))



baseline_sens_stats |> 
  bind_rows(reform_sens_stats) |> 
  ggplot(aes(time, equivalisedDisposableIncomeYearly_median, colour = scenario, fill = scenario)) +
  geom_vline(aes(xintercept = 2019, linetype = "Covid reform\nimplementation"),
             colour = "red") +
  stat_summary(
    fun.data = median_hilow,
    geom = "ribbon",
    alpha = 0.5,
    colour = NA
  ) +
  stat_summary(fun = median, geom = "line") +
  stat_summary(fun = median, geom = "point") +
  scale_fill_manual(
    "Policy:",
    aesthetics = c("fill", "colour"),
    breaks = c("baseline_sens", "reform_sens"),
    labels = c("Baseline", "Covid policy"),
    values = sphsu_cols("University Blue", "Rust", names = FALSE)
  ) +
  scale_linetype("") +
  xlab("Year") +
  scale_y_continuous(
    "Median disposable income",
    labels = scales::dollar_format(prefix = "£")
  ) +
  labs(
    caption = paste(
      "Notes:",
      "1,000 simulation runs in each condition.",
      "Red line denotes reform implementation point",
      sep = "\n"
    )
  ) +
  theme_sphsu_light() +
  theme(legend.position = "bottom",
        plot.caption = element_text(hjust = 0))
