package com.varun.ipl.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.varun.ipl.model.Match;
import com.varun.ipl.model.Team;
import com.varun.ipl.repository.MatchRepository;
import com.varun.ipl.repository.TeamRepository;

@RestController
public class TeamController {
	
	@Autowired
	private TeamRepository teamRepository;
	
	@Autowired
	private MatchRepository matchRepository;
	
	@GetMapping("/team/{teamName}")
	public Team getTeam(@PathVariable String teamName) {
		Team team = teamRepository.findByTeamName(teamName);
		
		List<Match> matches = matchRepository.getLatestMatches(teamName, 4);
		team.setMatches(matches);
		return team;
	}
}
