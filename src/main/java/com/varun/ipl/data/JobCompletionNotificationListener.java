package com.varun.ipl.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.transaction.Transactional;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.varun.ipl.model.Team;

@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

//	private final JdbcTemplate jdbcTemplate;

	private final EntityManager entityManager;

	@Autowired
	public JobCompletionNotificationListener(EntityManager entityManager) {
		this.entityManager = entityManager;
	}

	@Override
	@Transactional
	public void afterJob(JobExecution jobExecution) {
		if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
			System.out.println("JOB finished");
//			jdbcTemplate.query("select team1, team2, date from match", (rs, row) -> "Team1 : " + rs.getString(1) + "Team2: " + rs.getString(2) + "Team3: " + rs.getString(3))
//			.forEach(System.out::println);

			List<Object[]> results = entityManager
					.createQuery("select m.team1,count(*) from Match m group by m.team1", Object[].class)
					.getResultList();
			
			Map<String, Team> teamData = new HashMap<String, Team>();
			
			results.stream()
			  	   .map(result -> new Team((String)result[0], (long)result[1]))
			  	   .forEach(team -> teamData.put(team.getTeamName(), team));
			
			entityManager.createQuery("select m.team2,count(*) from Match m group by m.team2", Object[].class)
						 .getResultList()
						 .stream()
						 .map(result -> new Team((String)result[0], (long)result[1]))
						 .forEach(team -> {
							 Team currentTeam = teamData.get(team.getTeamName());
							 currentTeam.setTotalMatches(currentTeam.getTotalMatches() + team.getTotalMatches());
						 });
			
			entityManager.createQuery("select m.winner,count(*) from Match m group by m.winner", Object[].class)
			 .getResultList()
			 .stream()
			 .forEach(team -> {
				 Team currentTeam = teamData.get((String)team[0]);
				 if(currentTeam != null)currentTeam.setTotalWins((long)team[1]);
			 });
			 
			
			teamData.values().forEach(team -> entityManager.persist(team));
			teamData.values().forEach(System.out::println);
		}
	}

}
