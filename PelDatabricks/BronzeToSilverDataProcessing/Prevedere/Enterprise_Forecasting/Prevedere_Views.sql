-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("SourceCatalog", "")
-- MAGIC dbutils.widgets.text("TargetCatalog", "")

-- COMMAND ----------

Create schema if not exists $TargetCatalog.enterprise_forecasting;

-- COMMAND ----------

CREATE OR REPLACE VIEW $TargetCatalog.enterprise_forecasting.vw_prevedere_all_series as
SELECT
  ind.name provider_name,
  series.value,
  series.date,
  series.annotation,
  series.manuallyadjusted,
  series.isforecasted,
  series.providerid,
  series.runtime,
  case
    when series.runtime = series.last_runtime then 1
    else 0
  end cur_flag
from
  (
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_210885402
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_210885402
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_40845501
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_40845501
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_42597501
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_42597501
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_450248797
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_450248797
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_455493697
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_455493697
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_455493717
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_455493717
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_455493747
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_455493747
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_455493767
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_455493767
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_455493787
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_455493787
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_acmsno
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_acmsno
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_babanaics23saus
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_babanaics23saus
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_ces2000000007
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_ces2000000007
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_ces2000000011
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_ces2000000011
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_ces2023600002
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_ces2023600002
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_ces2023611856
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_ces2023611856
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_ces2023813001
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_ces2023813001
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_ces2023813002
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_ces2023813002
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_ceu0500000030
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_ceu0500000030
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_civpart
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_civpart
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_cpiaucns
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_cpiaucns
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_cpiaucsl
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_cpiaucsl
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_cpilfesl
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_cpilfesl
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_csushpinsa
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_csushpinsa
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_csushpisa
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_csushpisa
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_dgs10
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_dgs10
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_dgs30
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_dgs30
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_effr
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_effr
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_exhoslusm495s
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_exhoslusm495s
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_exsfhsusm495n
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_exsfhsusm495n
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_gthomeimprovementgeous
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_gthomeimprovementgeous
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_hnfsepussa
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_hnfsepussa
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_houst
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_houst
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_houst1f
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_houst1f
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_houst2f
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_houst2f
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_houst5f
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_houst5f
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_hsfmedusm052n
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_hsfmedusm052n
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_hsfsupusm673n
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_hsfsupusm673n
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_itimber
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_itimber
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_jcxfemd
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_jcxfemd
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_jhdusrgdpbr
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_jhdusrgdpbr
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_lnu04032231
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_lnu04032231
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_mortgage30us
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_mortgage30us
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_mpre8d6176bd59e346a0b2b847a32977617bm22157b9c051a4f6990c6fd509785bfb0
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_mpre8d6176bd59e346a0b2b847a32977617bm22157b9c051a4f6990c6fd509785bfb0
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_mpreindpromead0fa1490034360888179cf189e3f32
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_mpreindpromead0fa1490034360888179cf189e3f32
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_mprelaborparticipationbase
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_mprelaborparticipationbase
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_mprepermitm5d30d101a2444038992b6df8ac11fb3b
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_mprepermitm5d30d101a2444038992b6df8ac11fb3b
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_pcepi
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_pcepi
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_pcepilfe
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_pcepilfe
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_pellasfdasdfa
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_pellasfdasdfa
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_permit
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_permit
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_permit1
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_permit1
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_permit24
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_permit24
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_permit5
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_permit5
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_preva063rc1bas
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_preva063rc1bas
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevabimaster
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevabimaster
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevbjwc285193904baseline
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevbjwc285193904baseline
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevces0000000001base
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevces0000000001base
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevcomabi
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevcomabi
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevcpiaucslbaseline
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevcpiaucslbaseline
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevcpilfeslbas
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevcpilfeslbas
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevdodgecbpin
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevdodgecbpin
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevdodgeibpi
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevdodgeibpi
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevdodgemind
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevdodgemind
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevdspic96bas
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevdspic96bas
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevfpdapacpnsp111631q1closeunadjq1sfpbase
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevfpdapacpnsp111631q1closeunadjq1sfpbase
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevhoustbsln
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevhoustbsln
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevhoustpssmstc
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevhoustpssmstc
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevmcoilwticobsln
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevmcoilwticobsln
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevmonthrealgdpbsln
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevmonthrealgdpbsln
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevnahbwfhmi
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevnahbwfhmi
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevnpiarch
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevnpiarch
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevresabi
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevresabi
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevresabiins
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevresabiins
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevrsfsxmvbsln
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevrsfsxmvbsln
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevumcsentbsln
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevumcsentbsln
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_prevunratebas
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_prevunratebas
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_spsnsaus
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_spsnsaus
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_tlnrescons
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_tlnrescons
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_tlrescons
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_tlrescons
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_ttlcons
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_ttlcons
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_ucmsuo
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_ucmsuo
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_umcsent
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_umcsent
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_unrate
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_unrate
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_usq201a3s3
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_usq201a3s3
    union
    select
      value,
      date,
      annotation,
      manuallyadjusted,
      isforecasted,
      providerid,
      runtime,
      (
        select
          max(runtime)
        from
          $SourceCatalog.prevedere.series_ussthpi
      ) last_runtime
    from
      $SourceCatalog.prevedere.series_ussthpi
  ) series
  inner join $SourceCatalog.prevedere.indicator_metadata ind on series.providerid = ind.providerid

-- COMMAND ----------

CREATE OR REPLACE VIEW $TargetCatalog.enterprise_forecasting.vw_prevedere_current_series
AS
Select * from  $TargetCatalog.enterprise_forecasting.vw_prevedere_all_series where cur_flag =1
