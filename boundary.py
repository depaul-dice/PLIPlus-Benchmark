#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  3 11:15:53 2019

@author: tanu
"""
import distribution

h = 100
win_boundary=distribution.random_custDist(distribution.x0,distribution.x1,custDist=distribution.custDist1,size=h)
win_boundary[0] = distribution.x0
win_boundary.append(distribution.x1)
win_boundary.sort()
print(win_boundary)
print(len(win_boundary))

a = [0, 4.124894840100069, 5.340000125943689, 5.6190466761826485, 8.68116483061735, 12.311746000547863, 19.102190535694085, 20.054270761410585, 21.113728167697076, 21.897937157843273, 24.476472757934996, 26.372475951397412, 26.73078435280072, 28.211383944663215, 30.49784531532681, 35.657183601215856, 37.51784107741817, 40.88862106334305, 45.122467889079296, 47.95232869698207, 50.02032964358783, 50.12032964358783, 50.22032964358783, 50.32032964358783, 50.4075868969009, 50.449560367451916, 50.60272508558876, 50.913587774250914, 50.94572167864763, 51.06291064958245, 51.14135630528283, 51.2086666720088, 51.22309952326335, 51.26225703021709, 51.30917416495328, 51.34917416495328, 51.39792621136813, 51.42792621136813, 51.47792621136813, 51.492934604407324, 51.538011650600055, 51.59547611831569, 51.8415860522578, 51.96948863496965, 52.01736494828619, 52.03156359643476, 52.26058125581133, 52.2665994017387, 52.338561681370386, 52.39792621136813, 52.42134972769455, 52.47134972769455, 52.58041904780222, 52.68041904780222, 52.74024707364797, 52.84024707364797, 52.954465849274946, 53.125119597401905, 53.23637881660665, 53.3371602202079, 53.33966340022087, 53.34814481620229, 53.35539616489021, 53.537429587217446, 53.77578577423095, 53.90065206469291, 53.913889961858494, 54.06356445223211, 54.08027565848279, 54.10929579159447, 54.15056100827835, 54.31085728896197, 54.43329926127013, 54.66824290410861, 54.75094786602026, 54.77313225840985, 54.82405480491944, 54.85767917232881, 54.87844072479081, 54.90170541923424, 54.9922827907937,  58.15097066690047, 60.67925734272499, 64.11517982637407, 65.74350524450364, 69.9050630619637,  70.00864371711248, 73.3135425618148, 75.60839615511135, 76.83149327541491, 81.46558602425989, 82.20032819640977, 83.46661240466274, 85.1634131672535,  88.73720451773414, 90.90522645718664, 92.54569377379526, 93.0372761884081, 96.45104335424284, 98.53025880032027, 99.57904912651415, 100]
print(len(a))