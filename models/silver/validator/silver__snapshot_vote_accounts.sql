{{ config (
    materialized = 'view'
) }}

SELECT
  json_data:activatedStake::NUMBER AS activatedStake,
  json_data:commission::NUMBER AS commission,
  json_data:epochCredits[0][0]::NUMBER AS current_epoch,
  json_data:epochCredits as epochCredits,
  json_data:epochVoteAccount::BOOLEAN AS epochVoteAccount,
  json_data:lastVote::NUMBER AS lastVote,
  json_data:nodePubkey::STRING AS nodePubkey,
  json_data:rootSlot::NUMBER AS rootSlot,
  json_data:votePubkey::STRING AS votePubkey,
  _inserted_timestamp
FROM
  solana_dev.bronze.temp_final_vote_accounts
