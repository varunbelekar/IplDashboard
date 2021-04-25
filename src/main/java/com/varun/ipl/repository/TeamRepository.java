package com.varun.ipl.repository;

import org.springframework.data.repository.CrudRepository;

import com.varun.ipl.model.Team;

public interface TeamRepository extends CrudRepository<Team, Long>{
	
	Team findByTeamName(String teamName);
}
