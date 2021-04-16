import os
import logging
from slack.web.client import WebClient
from slack.errors import SlackApiError
from sqlalchemy import create_engine
import pandas as pd

"""
    SELECT	
        r.id,
		(now() - k.received_at) as TAT,
		r.status,
		t.id as ticket_id,
		to_char(k.received_at AT TIME ZONE 'UTC' AT TIME ZONE 'EDT', 'MM/DD/YYYY HH12:MI') as kit_received_at,
		to_char(s.collection_date AT TIME ZONE 'UTC' AT TIME ZONE 'EDT', 'MM/DD/YYYY HH12:MI') as sample_collection_date,
        r.sent_date,
		s.id
		--sds.id
		
		FROM
        reports r
    	join kits k on k.id = r.kit_id
		left join tickets t on t.report_id = r.id
		join samples s on s.kit_id = r.kit_id
		--join sample_data_sources sds on sds.sample_id = s.id
		join distributors d on r.cached_distributor_id = d.id
		
        WHERE
        r.status IN ('awaiting_results','awaiting_review','approved')
        and r.created_at > now() - interval '2 weeks'
    	and r.report_type_id = 283
		--and not r.expedited
		and k.received_at is not null
    	and (now() - k.received_at) >= interval '48 hours'
		--and t.category in ('Critical Missing Information', 'Missing Information')
		AND (d.parent_distributor_id = 15 or r.cached_distributor_id = 15)
		
		order by (now() - k.received_at) desc
"""       


def main(test):
    approve_these_samples="""
    SELECT	
    	r.id,
    	(now() - k.received_at) as TAT,
    	r.status,
    	t.id as ticket_id,
    	to_char(k.received_at AT TIME ZONE 'UTC' AT TIME ZONE 'EDT', 'MM/DD/YYYY HH12:MI') as kit_received_at,
    	to_char(s.collection_date AT TIME ZONE 'UTC' AT TIME ZONE 'EDT', 'MM/DD/YYYY HH12:MI') as sample_collection_date,
    	r.sent_date,
    	s.id
    	--sds.id
    
    	FROM
    	reports r
    	join kits k on k.id = r.kit_id
    	left join tickets t on t.report_id = r.id
    	join samples s on s.kit_id = r.kit_id
    	--join sample_data_sources sds on sds.sample_id = s.id
    	join distributors d on r.cached_distributor_id = d.id
    
    	WHERE
    	r.status IN ('awaiting_results','awaiting_review','approved')
    	and r.created_at > now() - interval '2 weeks'
    	and r.report_type_id = 283
    	--and not r.expedited
    	and k.received_at is not null
    	and (now() - k.received_at) >= interval '48 hours'
    	--and t.category in ('Critical Missing Information', 'Missing Information')
    	AND (d.parent_distributor_id = 15 or r.cached_distributor_id = 15)
    """
    
    
    covid_reports_approaching_tat="""
    SELECT	
    	distinct r.id,
    	(now() - s.received_at) as TAT,
    	r.status,
    	t.id as ticket_id,
    	to_char(s.received_at AT TIME ZONE 'UTC' AT TIME ZONE 'EDT', 'MM/DD/YYYY HH12:MI') as sample_received_at,
    	to_char(s.collection_date AT TIME ZONE 'UTC' AT TIME ZONE 'EDT', 'MM/DD/YYYY HH12:MI') as sample_collection_date,
    	r.sent_date,
    	s.id as sample_id
    	--sds.id
    
    	FROM
    	reports r
    	join kits k on k.id = r.kit_id
    	left join tickets t on t.report_id = r.id
    	join samples s on s.kit_id = r.kit_id
    	--join sample_data_sources sds on sds.sample_id = s.id
    	join clinics c on r.clinic_id = c.id
    	join distributors d on c.distributor_id = d.id
        left join report_sample_data_sources rsds on rsds.report_id = r.id
    
    	WHERE
    	r.status IN ('awaiting_results','awaiting_review','approved')
    	and r.created_at > now() - interval '2 weeks'
    	and r.report_type_id = 283
    	and s.received_at is not null
    	and c.id <> 2902 --Missing Info Clinic
    	--and t.category in ('Critical Missing Information', 'Missing Information')
    	AND (d.parent_distributor_id = 15 or r.cached_distributor_id = 15)
        AND rsds.id  is not null
        """
        
    regular_approaching_tat = covid_reports_approaching_tat + "\n and not r.expedited \n and (now() - s.received_at) >= interval '32 hours'"
        
    stat_approaching_tat = covid_reports_approaching_tat + "\n and r.expedited \n and (now() - s.received_at) >= interval '16 hours'"
    
    STAT_samples_not_plated="""
    SELECT distinct r.id, pu.status, k.status, r.status, k.received_at AT TIME ZONE 'EDT'
    from reports r
    join patient_users pu on pu.id = r.patient_user_id
    join kits k on k.id = r.kit_id
    where
    r.created_at > '1-1-21'
    and k.received_at is not null
    and pu.status = 'sample_received_at_lab'
    and r.report_type_id = 283
    and r.status IN ('awaiting_results','awaiting_review','approved')
    and NOW() - k.received_at > INTERVAL '6 hours'
    and r.expedited
        """
    
    
    def get_report_ids(sql):
        con = create_engine(os.environ["FOLLOWER_DB_URL"])
        df = list(pd.read_sql_query(sql,con)['id'])
        return df
    
    list_of_regular_TAT_ids = get_report_ids(regular_approaching_tat)
    list_of_STAT_TAT_ids = get_report_ids(stat_approaching_tat)
    
    list_of_unplated_stat_ids = get_report_ids(STAT_samples_not_plated)
    
    lists_of_ids_to_check = ['list_of_regular_TAT_ids','list_of_STAT_TAT_ids','list_of_unplated_stat_ids']
    
    def sendMessage(msg, test):
      SLACK_BOT_TOKEN = os.environ['TAT_alerts_slack_bot_token']
      slack_client = WebClient(SLACK_BOT_TOKEN)
      logging.debug("authorized slack client")
      # make the POST request through the python slack client
      
      channelname = "#tat-alerts"
      if test:
          channelname = channelname + '-test'
      # check if the request was a success
      try:
        slack_client.chat_postMessage(
          channel=channelname,
          text=msg
        )
      except SlackApiError as e:
        logging.error('Request to Slack API Failed: {}.'.format(e.response.status_code))
        logging.error(e.response)
    
    msg = ''
    
    if len(list_of_regular_TAT_ids)>0:
        msg = msg + "\n COVID reports requiring attention:"
        for x in list_of_regular_TAT_ids:
            url = "https://elements.phosphorus.com/reporting/reports/{}/edit".format(x)
            msg = msg + "\n <{url}|{id}>".format(**{'id':x,'url':url})
    
    if len(list_of_STAT_TAT_ids)>0:
        msg = msg + "\n STAT COVID reports requiring attention:"
        for x in list_of_STAT_TAT_ids:
            url = "https://elements.phosphorus.com/reporting/reports/{}/edit".format(x)
            msg = msg + "\n <{url}|{id}> (STAT)".format(**{'id':x,'url':url})
            

        
    if len(list_of_unplated_stat_ids)>0:
        msg = msg + "\n The following STAT samples were received more than 6 hours ago and have not been plated:"
        for x in list_of_unplated_stat_ids:
            url = "https://elements.phosphorus.com/reporting/reports/{}/edit".format(x)
            msg = msg + "\n <{url}|{id}>".format(**{'id':x,'url':url})
    

        
    if msg=='':
        sendMessage('Just checked. Nothing to report.',test)
    elif test:
        sendMessage(msg,test)
    else:
        sendMessage("<!here>" + msg,test)
        
    logging.basicConfig(level=logging.DEBUG)


if __name__ == "__main__":
    '''
    test = input("Test? [Y/N]")
    if "Y" in test.upper():
        test=True
    else:
        test=False
    '''
    test=False
    main(test)

